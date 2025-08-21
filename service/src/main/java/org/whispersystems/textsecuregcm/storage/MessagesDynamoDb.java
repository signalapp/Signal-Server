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
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
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

  @VisibleForTesting
  static final int MAY_HAVE_URGENT_MESSAGES_QUERY_LIMIT = 20;

  private static final String KEY_TTL = "E";
  private static final String KEY_ENVELOPE_BYTES = "EB";

  private final Timer storeTimer = timer(name(getClass(), "store"));

  private final DynamoDbAsyncClient dbAsyncClient;
  private final String tableName;
  private final Duration timeToLive;
  private final ExecutorService messageDeletionExecutor;
  private final Scheduler messageDeletionScheduler;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private static final Logger logger = LoggerFactory.getLogger(MessagesDynamoDb.class);

  public MessagesDynamoDb(DynamoDbClient dynamoDb, DynamoDbAsyncClient dynamoDbAsyncClient, String tableName,
      Duration timeToLive, ExecutorService messageDeletionExecutor,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {
    super(dynamoDb);

    this.dbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
    this.timeToLive = timeToLive;

    this.messageDeletionExecutor = messageDeletionExecutor;
    this.messageDeletionScheduler = Schedulers.fromExecutor(messageDeletionExecutor);
    this.experimentEnrollmentManager = experimentEnrollmentManager;
  }

  public void store(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid,
      final Device destinationDevice) {
    storeTimer.record(() -> writeInBatches(messages, (messageBatch) -> storeBatch(messageBatch, destinationAccountUuid, destinationDevice)));
  }

  private void storeBatch(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid,
      final Device destinationDevice) {
    if (messages.size() > DYNAMO_DB_MAX_BATCH_SIZE) {
      throw new IllegalArgumentException("Maximum batch size of " + DYNAMO_DB_MAX_BATCH_SIZE + " exceeded with " + messages.size() + " messages");
    }

    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, destinationDevice);
    List<WriteRequest> writeItems = new ArrayList<>();
    for (MessageProtos.Envelope message : messages) {
      final UUID messageUuid = UUID.fromString(message.getServerGuid());

      final ImmutableMap.Builder<String, AttributeValue> item = ImmutableMap.<String, AttributeValue>builder()
          .put(KEY_PARTITION, partitionKey)
          .put(KEY_SORT, convertSortKey(message.getServerTimestamp(), messageUuid))
          .put(LOCAL_INDEX_MESSAGE_UUID_KEY_SORT, convertLocalIndexMessageUuidSortKey(messageUuid))
          .put(KEY_TTL, AttributeValues.fromLong(getTtlForMessage(message)))
          .put(KEY_ENVELOPE_BYTES, AttributeValue.builder().b(SdkBytes.fromByteArray(EnvelopeUtil.compress(message).toByteArray())).build());

      writeItems.add(WriteRequest.builder().putRequest(PutRequest.builder()
          .item(item.build())
          .build()).build());
    }

    executeTableWriteItemsUntilComplete(Map.of(tableName, writeItems));
  }

  public CompletableFuture<Boolean> mayHaveMessages(final UUID accountIdentifier, final Device device) {
    return dbAsyncClient.query(QueryRequest.builder()
            .tableName(tableName)
            .consistentRead(false)
            .limit(1)
            .keyConditionExpression("#part = :part")
            .expressionAttributeNames(Map.of("#part", KEY_PARTITION))
            .expressionAttributeValues(Map.of(":part", convertPartitionKey(accountIdentifier, device))).build())
        .thenApply(queryResponse -> queryResponse.count() > 0);
  }

  public CompletableFuture<Boolean> mayHaveUrgentMessages(final UUID accountIdentifier, final Device device) {
    return Flux.from(load(accountIdentifier, device, MAY_HAVE_URGENT_MESSAGES_QUERY_LIMIT))
        .any(MessageProtos.Envelope::getUrgent)
        .toFuture();
  }

  public Publisher<MessageProtos.Envelope> load(final UUID destinationAccountUuid, final Device device, final Integer limit) {
    QueryRequest.Builder queryRequestBuilder = QueryRequest.builder()
        .tableName(tableName)
        .consistentRead(true)
        .keyConditionExpression("#part = :part")
        .expressionAttributeNames(Map.of("#part", KEY_PARTITION))
        .expressionAttributeValues(Map.of(":part", convertPartitionKey(destinationAccountUuid, device)));

    if (limit != null) {
      // some callers don’t take advantage of reactive streams, so we want to support limiting the fetch size. Otherwise,
      // we could fetch up to 1 MB (likely >1,000 messages) and discard 90% of them
      queryRequestBuilder.limit(Math.min(RESULT_SET_CHUNK_SIZE, limit));
    }

    final QueryRequest queryRequest = queryRequestBuilder.build();

    return dbAsyncClient.queryPaginator(queryRequest).items()
        .map(message -> {
          try {
            return convertItemToEnvelope(message, experimentEnrollmentManager);
          } catch (final InvalidProtocolBufferException e) {
            logger.error("Failed to parse envelope", e);
            return null;
          }
        })
        .filter(Predicate.not(Objects::isNull));
  }

  public CompletableFuture<Optional<MessageProtos.Envelope>> deleteMessageByDestinationAndGuid(
      final UUID destinationAccountUuid, final Device destinationDevice, final UUID messageUuid) {
    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid, destinationDevice);
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
              return convertItemToEnvelope(deleteItemResponse.attributes(), experimentEnrollmentManager);
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
    DeleteItemRequest.Builder deleteItemRequest = DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_PARTITION, convertPartitionKey(destinationAccountUuid, destinationDevice), KEY_SORT, convertSortKey(serverTimestamp, messageUuid)))
        .returnValues(ReturnValue.ALL_OLD);

    return dbAsyncClient.deleteItem(deleteItemRequest.build())
        .thenApplyAsync(deleteItemResponse -> {
          if (deleteItemResponse.attributes() != null && deleteItemResponse.attributes().containsKey(KEY_PARTITION)) {
            try {
              return Optional.of(convertItemToEnvelope(deleteItemResponse.attributes(), experimentEnrollmentManager));
            } catch (final InvalidProtocolBufferException e) {
              logger.error("Failed to parse envelope", e);
            }
          }

          return Optional.empty();
        }, messageDeletionExecutor);
  }

  @VisibleForTesting
  static MessageProtos.Envelope convertItemToEnvelope(final Map<String, AttributeValue> item,
      final ExperimentEnrollmentManager experimentEnrollmentManager) throws InvalidProtocolBufferException {

    return EnvelopeUtil.expand(MessageProtos.Envelope.parseFrom(item.get(KEY_ENVELOPE_BYTES).b().asByteArray()),
        experimentEnrollmentManager);
  }

  private long getTtlForMessage(MessageProtos.Envelope message) {
    return message.getServerTimestamp() / 1000 + timeToLive.getSeconds();
  }

  private static AttributeValue convertPartitionKey(final UUID destinationAccountUuid, final Device destinationDevice) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(24);
    byteBuffer.putLong(destinationAccountUuid.getMostSignificantBits());
    byteBuffer.putLong(destinationAccountUuid.getLeastSignificantBits());
    byteBuffer.putLong((destinationDevice.getCreated() & ~0x7f) + destinationDevice.getId());
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  private static AttributeValue convertSortKey(final long serverTimestamp, final UUID messageUuid) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(24);
    byteBuffer.putLong(serverTimestamp);
    byteBuffer.putLong(messageUuid.getMostSignificantBits());
    byteBuffer.putLong(messageUuid.getLeastSignificantBits());
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  private static AttributeValue convertLocalIndexMessageUuidSortKey(final UUID messageUuid) {
    return AttributeValues.fromUUID(messageUuid);
  }
}
