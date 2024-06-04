/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;
import static io.micrometer.core.instrument.Metrics.timer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessagesConfiguration.DynamoKeyScheme;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;
import reactor.util.function.Tuple2;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class MessagesDynamoDb extends AbstractDynamoDbStore {

  @VisibleForTesting
  static final String KEY_PARTITION = "H";

  @VisibleForTesting
  static final String KEY_SORT = "S";

  @VisibleForTesting
  static final String LOCAL_INDEX_MESSAGE_UUID_NAME = "Message_UUID_Index";

  @VisibleForTesting
  static final String LOCAL_INDEX_MESSAGE_UUID_KEY_SORT = "U";

  private static final String KEY_TTL = "E";
  private static final String KEY_ENVELOPE_BYTES = "EB";

  private final Timer storeTimer = timer(name(getClass(), "store"));
  private final String DELETE_BY_ACCOUNT_TIMER_NAME = name(getClass(), "delete", "account");
  private final String DELETE_BY_DEVICE_TIMER_NAME = name(getClass(), "delete", "device");
  private final String MESSAGES_STORED_BY_SCHEME_COUNTER_NAME = name(getClass(), "messagesStored");
  private final String MESSAGES_LOADED_BY_SCHEME_COUNTER_NAME = name(getClass(), "messagesLoaded");
  private final String MESSAGES_DELETED_BY_SCHEME_COUNTER_NAME = name(getClass(), "messagesDeleted");

  private final DynamoDbAsyncClient dbAsyncClient;
  private final String tableName;
  private final Duration timeToLive;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfig;
  private final ExecutorService messageDeletionExecutor;
  private final Scheduler messageDeletionScheduler;

  private static final Logger logger = LoggerFactory.getLogger(MessagesDynamoDb.class);

  public MessagesDynamoDb(DynamoDbClient dynamoDb, DynamoDbAsyncClient dynamoDbAsyncClient, String tableName,
      Duration timeToLive, DynamicConfigurationManager<DynamicConfiguration> dynamicConfig, ExecutorService messageDeletionExecutor) {
    super(dynamoDb);

    this.dbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
    this.timeToLive = timeToLive;
    this.dynamicConfig = dynamicConfig;

    this.messageDeletionExecutor = messageDeletionExecutor;
    this.messageDeletionScheduler = Schedulers.fromExecutor(messageDeletionExecutor);
  }

  public void store(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid,
      final Device destinationDevice) {
    storeTimer.record(() -> writeInBatches(messages, (messageBatch) -> storeBatch(messageBatch, destinationAccountUuid, destinationDevice)));
  }

  private void storeBatch(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid,
      final Device destinationDevice) {
    final byte destinationDeviceId = destinationDevice.getId();
    if (messages.size() > DYNAMO_DB_MAX_BATCH_SIZE) {
      throw new IllegalArgumentException("Maximum batch size of " + DYNAMO_DB_MAX_BATCH_SIZE + " exceeded with " + messages.size() + " messages");
    }

    final DynamoKeyScheme scheme = dynamicConfig.getConfiguration().getMessagesConfiguration().writeKeyScheme();
    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, destinationDevice, scheme);
    List<WriteRequest> writeItems = new ArrayList<>();
    for (MessageProtos.Envelope message : messages) {
      final UUID messageUuid = UUID.fromString(message.getServerGuid());

      final ImmutableMap.Builder<String, AttributeValue> item = ImmutableMap.<String, AttributeValue>builder()
          .put(KEY_PARTITION, partitionKey)
          .put(KEY_SORT, convertSortKey(destinationDevice.getId(), message.getServerTimestamp(), messageUuid, scheme))
          .put(LOCAL_INDEX_MESSAGE_UUID_KEY_SORT, convertLocalIndexMessageUuidSortKey(messageUuid))
          .put(KEY_TTL, AttributeValues.fromLong(getTtlForMessage(message)))
          .put(KEY_ENVELOPE_BYTES, AttributeValue.builder().b(SdkBytes.fromByteArray(message.toByteArray())).build());

      writeItems.add(WriteRequest.builder().putRequest(PutRequest.builder()
          .item(item.build())
          .build()).build());
    }

    executeTableWriteItemsUntilComplete(Map.of(tableName, writeItems));
    Metrics.counter(MESSAGES_STORED_BY_SCHEME_COUNTER_NAME, Tags.of("scheme", scheme.name())).increment(writeItems.size());
  }

  public Publisher<MessageProtos.Envelope> load(final UUID destinationAccountUuid, final Device device, final Integer limit) {
    return Flux.concat(
        dynamicConfig.getConfiguration().getMessagesConfiguration().dynamoKeySchemes()
            .stream()
            .map(scheme -> load(destinationAccountUuid, device, limit, scheme))
            .toList())
        .map(messageAndScheme -> {
              Metrics.counter(MESSAGES_LOADED_BY_SCHEME_COUNTER_NAME, Tags.of("scheme", messageAndScheme.getT2().name())).increment();
              return messageAndScheme.getT1();
            });
  }

  private Publisher<Tuple2<MessageProtos.Envelope, DynamoKeyScheme>> load(final UUID destinationAccountUuid, final Device device, final Integer limit, final DynamoKeyScheme scheme) {
    final byte destinationDeviceId = device.getId();

    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, device, scheme);
    QueryRequest.Builder queryRequestBuilder = QueryRequest.builder()
        .tableName(tableName)
        .consistentRead(true);

    queryRequestBuilder = switch (scheme) {
      case TRADITIONAL -> queryRequestBuilder
          .keyConditionExpression("#part = :part AND begins_with ( #sort , :sortprefix )")
          .expressionAttributeNames(Map.of(
              "#part", KEY_PARTITION,
              "#sort", KEY_SORT))
          .expressionAttributeValues(Map.of(
              ":part", partitionKey,
              ":sortprefix", convertDestinationDeviceIdToSortKeyPrefix(destinationDeviceId, scheme)));
      case LAZY_DELETION -> queryRequestBuilder
          .keyConditionExpression("#part = :part")
          .expressionAttributeNames(Map.of("#part", KEY_PARTITION))
          .expressionAttributeValues(Map.of(":part", partitionKey));
    };

    if (limit != null) {
      // some callers don’t take advantage of reactive streams, so we want to support limiting the fetch size. Otherwise,
      // we could fetch up to 1 MB (likely >1,000 messages) and discard 90% of them
      queryRequestBuilder.limit(Math.min(RESULT_SET_CHUNK_SIZE, limit));
    }

    final QueryRequest queryRequest = queryRequestBuilder.build();

    return dbAsyncClient.queryPaginator(queryRequest).items()
        .map(message -> {
          try {
            return convertItemToEnvelope(message);
          } catch (final InvalidProtocolBufferException e) {
            logger.error("Failed to parse envelope", e);
            return null;
          }
        })
        .filter(Predicate.not(Objects::isNull))
        .map(m -> Tuples.of(m, scheme));
  }

  public CompletableFuture<Optional<MessageProtos.Envelope>> deleteMessageByDestinationAndGuid(
      final UUID destinationAccountUuid, final Device destinationDevice, final UUID messageUuid) {
    return dynamicConfig.getConfiguration().getMessagesConfiguration().dynamoKeySchemes()
        .stream()
        .map(scheme -> deleteMessageByDestinationAndGuid(destinationAccountUuid, destinationDevice, messageUuid, scheme))
        // this combines the futures by producing a future that returns an arbitrary nonempty
        // result if there is one, which should be OK because only one of the keying schemes
        // should produce a nonempty result for any given message uuid
        .reduce((f, g) -> f.thenCombine(g, (a, b) -> a.or(() -> b)))
        .get();                 // there is always at least one scheme
  }

  private CompletableFuture<Optional<MessageProtos.Envelope>> deleteMessageByDestinationAndGuid(
      final UUID destinationAccountUuid, final Device destinationDevice, final UUID messageUuid, DynamoKeyScheme scheme) {
    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, destinationDevice, scheme);
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(tableName)
        .indexName(LOCAL_INDEX_MESSAGE_UUID_NAME)
        .projectionExpression(KEY_SORT)
        .consistentRead(true)
        .keyConditionExpression("#part = :part AND #uuid = :uuid")
        .expressionAttributeNames(Map.of(
            "#part", KEY_PARTITION,
            "#uuid", LOCAL_INDEX_MESSAGE_UUID_KEY_SORT))
        .expressionAttributeValues(Map.of(
            ":part", partitionKey,
            ":uuid", convertLocalIndexMessageUuidSortKey(messageUuid)))
        .build();

    // because we are filtering on message UUID, this query should return at most one item,
    // but it’s simpler to handle the full stream and return the “last” item
    return Flux.from(dbAsyncClient.queryPaginator(queryRequest).items())
        .flatMap(item -> Mono.fromCompletionStage(dbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_PARTITION, partitionKey, KEY_SORT,
                AttributeValues.fromByteArray(item.get(KEY_SORT).b().asByteArray())))
            .returnValues(ReturnValue.ALL_OLD)
            .build())))
        .mapNotNull(deleteItemResponse -> {
          try {
            if (deleteItemResponse.attributes() != null && deleteItemResponse.attributes().containsKey(KEY_PARTITION)) {
              Metrics.counter(MESSAGES_DELETED_BY_SCHEME_COUNTER_NAME, Tags.of("scheme", scheme.name())).increment();
              return convertItemToEnvelope(deleteItemResponse.attributes());
            }
          } catch (final InvalidProtocolBufferException e) {
            logger.error("Failed to parse envelope", e);
          }
          return null;
        })
        .map(Optional::ofNullable)
        .subscribeOn(messageDeletionScheduler)
        .last(Optional.empty()) // if the flux is empty, last() will throw without a default
        .toFuture();
  }

  public CompletableFuture<Optional<MessageProtos.Envelope>> deleteMessage(final UUID destinationAccountUuid,
      final Device destinationDevice, final UUID messageUuid, final long serverTimestamp) {
    return dynamicConfig.getConfiguration().getMessagesConfiguration().dynamoKeySchemes()
        .stream()
        .map(scheme -> deleteMessage(destinationAccountUuid, destinationDevice, messageUuid, serverTimestamp, scheme))
        // this combines the futures by producing a future that returns an arbitrary nonempty
        // result if there is one, which should be OK because only one of the keying schemes
        // should produce a nonempty result for any given message uuid
        .reduce((f, g) -> f.thenCombine(g, (a, b) -> a.or(() -> b)))
        .orElseThrow();         // there is always at least one scheme
  }

  private CompletableFuture<Optional<MessageProtos.Envelope>> deleteMessage(final UUID destinationAccountUuid,
      final Device destinationDevice, final UUID messageUuid, final long serverTimestamp, final DynamoKeyScheme scheme) {
    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, destinationDevice, scheme);
    final AttributeValue sortKey = convertSortKey(destinationDevice.getId(), serverTimestamp, messageUuid, scheme);
    DeleteItemRequest.Builder deleteItemRequest = DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_PARTITION, partitionKey, KEY_SORT, sortKey))
        .returnValues(ReturnValue.ALL_OLD);

    return dbAsyncClient.deleteItem(deleteItemRequest.build())
        .thenApplyAsync(deleteItemResponse -> {
          if (deleteItemResponse.attributes() != null && deleteItemResponse.attributes().containsKey(KEY_PARTITION)) {
            try {
              return Optional.of(convertItemToEnvelope(deleteItemResponse.attributes()));
            } catch (final InvalidProtocolBufferException e) {
              logger.error("Failed to parse envelope", e);
            }
          }

          return Optional.empty();
        }, messageDeletionExecutor);
  }

  // Deletes all messages stored for the supplied account that were stored under the traditional (uuid+device id) keying scheme.
  // Messages stored under the lazy-message-deletion keying scheme will not be affected.
  public CompletableFuture<Void> deleteAllMessagesForAccount(final UUID destinationAccountUuid) {
    final Timer.Sample sample = Timer.start();

    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, null, DynamoKeyScheme.TRADITIONAL);

    return Flux.from(dbAsyncClient.queryPaginator(QueryRequest.builder()
            .tableName(tableName)
            .projectionExpression(KEY_SORT)
            .consistentRead(true)
            .keyConditionExpression("#part = :part")
            .expressionAttributeNames(Map.of("#part", KEY_PARTITION))
            .expressionAttributeValues(Map.of(":part", partitionKey))
            .build())
        .items())
        .flatMap(item -> Mono.fromFuture(() -> dbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_PARTITION, partitionKey,
                KEY_SORT, item.get(KEY_SORT)))
            .build())),
            DYNAMO_DB_MAX_BATCH_SIZE)
        .then()
        .doOnSuccess(ignored -> sample.stop(timer(DELETE_BY_ACCOUNT_TIMER_NAME, "outcome", "success")))
        .doOnError(ignored -> sample.stop(timer(DELETE_BY_ACCOUNT_TIMER_NAME, "outcome", "error")))
        .toFuture();
  }

  // Deletes all messages stored for the supplied account and device that were stored under the
  // traditional (uuid+device id) keying scheme. Messages stored under the lazy-message-deletion
  // keying scheme will not be affected.
  public CompletableFuture<Void> deleteAllMessagesForDevice(final UUID destinationAccountUuid,
      final byte destinationDeviceId) {
    final Timer.Sample sample = Timer.start();
    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, null, DynamoKeyScheme.TRADITIONAL);

    return Flux.from(dbAsyncClient.queryPaginator(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#part = :part AND begins_with ( #sort , :sortprefix )")
                .expressionAttributeNames(Map.of(
                    "#part", KEY_PARTITION,
                    "#sort", KEY_SORT))
                .expressionAttributeValues(Map.of(
                    ":part", partitionKey,
                    ":sortprefix", convertDestinationDeviceIdToSortKeyPrefix(destinationDeviceId, DynamoKeyScheme.TRADITIONAL)))
                .projectionExpression(KEY_SORT)
                .consistentRead(true)
                .build())
            .items())
        .flatMap(item -> Mono.fromFuture(() -> dbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_PARTITION, partitionKey,
                KEY_SORT, item.get(KEY_SORT)))
            .build())),
            DYNAMO_DB_MAX_BATCH_SIZE)
        .then()
        .doOnSuccess(ignored -> sample.stop(timer(DELETE_BY_DEVICE_TIMER_NAME, "outcome", "success")))
        .doOnError(ignored -> sample.stop(timer(DELETE_BY_DEVICE_TIMER_NAME, "outcome", "error")))
        .toFuture();
  }

  @VisibleForTesting
  static MessageProtos.Envelope convertItemToEnvelope(final Map<String, AttributeValue> item)
      throws InvalidProtocolBufferException {

    return MessageProtos.Envelope.parseFrom(item.get(KEY_ENVELOPE_BYTES).b().asByteArray());
  }

  private long getTtlForMessage(MessageProtos.Envelope message) {
    return message.getServerTimestamp() / 1000 + timeToLive.getSeconds();
  }

  private static AttributeValue convertPartitionKey(final UUID destinationAccountUuid, final Device destinationDevice, final DynamoKeyScheme scheme) {
    return switch (scheme) {
      case TRADITIONAL -> AttributeValues.fromUUID(destinationAccountUuid);
      case LAZY_DELETION -> {
          final ByteBuffer byteBuffer = ByteBuffer.allocate(24);
          byteBuffer.putLong(destinationAccountUuid.getMostSignificantBits());
          byteBuffer.putLong(destinationAccountUuid.getLeastSignificantBits());
          byteBuffer.putLong(destinationDevice.getCreated() & ~0x7f + destinationDevice.getId());
          yield AttributeValues.fromByteBuffer(byteBuffer.flip());
      }
    };
  }

  private static AttributeValue convertSortKey(final byte destinationDeviceId, final long serverTimestamp,
      final UUID messageUuid, final DynamoKeyScheme scheme) {

    final ByteBuffer byteBuffer = ByteBuffer.allocate(32);
    if (scheme == DynamoKeyScheme.TRADITIONAL) {
        // for compatibility - destinationDeviceId was previously `long`
        byteBuffer.putLong(destinationDeviceId);
    }
    byteBuffer.putLong(serverTimestamp);
    byteBuffer.putLong(messageUuid.getMostSignificantBits());
    byteBuffer.putLong(messageUuid.getLeastSignificantBits());
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  private static AttributeValue convertDestinationDeviceIdToSortKeyPrefix(final byte destinationDeviceId, final DynamoKeyScheme scheme) {
    return switch (scheme) {
      case TRADITIONAL -> AttributeValues.fromByteBuffer(ByteBuffer.allocate(8).putLong(destinationDeviceId).flip());
      case LAZY_DELETION -> AttributeValues.b(new byte[0]);
    };
  }

  private static AttributeValue convertLocalIndexMessageUuidSortKey(final UUID messageUuid) {
    return AttributeValues.fromUUID(messageUuid);
  }
}
