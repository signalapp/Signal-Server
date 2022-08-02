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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class MessagesDynamoDb extends AbstractDynamoDbStore {

  private static final String KEY_PARTITION = "H";
  private static final String KEY_SORT = "S";

  private static final String LOCAL_INDEX_MESSAGE_UUID_NAME = "Message_UUID_Index";
  private static final String LOCAL_INDEX_MESSAGE_UUID_KEY_SORT = "U";

  private static final String KEY_TTL = "E";
  private static final String KEY_ENVELOPE_BYTES = "EB";

  // TODO Stop reading messages by attribute value after DATE
  @Deprecated
  private static final String KEY_TYPE = "T";

  @Deprecated
  private static final String KEY_TIMESTAMP = "TS";
  private static final String KEY_SOURCE_UUID = "SU";

  @Deprecated
  private static final String KEY_SOURCE_DEVICE = "SD";

  @Deprecated
  private static final String KEY_DESTINATION_UUID = "DU";

  @Deprecated
  private static final String KEY_UPDATED_PNI = "UP";

  @Deprecated
  private static final String KEY_CONTENT = "C";

  private final Timer storeTimer = timer(name(getClass(), "store"));
  private final Timer loadTimer = timer(name(getClass(), "load"));
  private final Timer deleteByGuid = timer(name(getClass(), "delete", "guid"));
  private final Timer deleteByKey = timer(name(getClass(), "delete", "key"));
  private final Timer deleteByAccount = timer(name(getClass(), "delete", "account"));
  private final Timer deleteByDevice = timer(name(getClass(), "delete", "device"));

  private final String tableName;
  private final Duration timeToLive;

  private static final Counter GET_MESSAGE_WITH_ATTRIBUTES_COUNTER = Metrics.counter(name(MessagesDynamoDb.class, "loadMessage"), "format", "attributes");
  private static final Counter GET_MESSAGE_WITH_ENVELOPE_COUNTER = Metrics.counter(name(MessagesDynamoDb.class, "loadMessage"), "format", "envelope");

  private static final Logger logger = LoggerFactory.getLogger(MessagesDynamoDb.class);

  public MessagesDynamoDb(DynamoDbClient dynamoDb, String tableName, Duration timeToLive) {
    super(dynamoDb);

    this.tableName = tableName;
    this.timeToLive = timeToLive;
  }

  public void store(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid, final long destinationDeviceId) {
    storeTimer.record(() -> writeInBatches(messages, (messageBatch) -> storeBatch(messageBatch, destinationAccountUuid, destinationDeviceId)));
  }

  private void storeBatch(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid, final long destinationDeviceId) {
    if (messages.size() > DYNAMO_DB_MAX_BATCH_SIZE) {
      throw new IllegalArgumentException("Maximum batch size of " + DYNAMO_DB_MAX_BATCH_SIZE + " exceeded with " + messages.size() + " messages");
    }

    final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid);
    List<WriteRequest> writeItems = new ArrayList<>();
    for (MessageProtos.Envelope message : messages) {
      final UUID messageUuid = UUID.fromString(message.getServerGuid());

      final ImmutableMap.Builder<String, AttributeValue> item = ImmutableMap.<String, AttributeValue>builder()
          .put(KEY_PARTITION, partitionKey)
          .put(KEY_SORT, convertSortKey(destinationDeviceId, message.getServerTimestamp(), messageUuid))
          .put(LOCAL_INDEX_MESSAGE_UUID_KEY_SORT, convertLocalIndexMessageUuidSortKey(messageUuid))
          .put(KEY_TTL, AttributeValues.fromLong(getTtlForMessage(message)))
          .put(KEY_ENVELOPE_BYTES, AttributeValue.builder().b(SdkBytes.fromByteArray(message.toByteArray())).build());

      writeItems.add(WriteRequest.builder().putRequest(PutRequest.builder()
          .item(item.build())
          .build()).build());
    }

    executeTableWriteItemsUntilComplete(Map.of(tableName, writeItems));
  }

  public List<MessageProtos.Envelope> load(final UUID destinationAccountUuid, final long destinationDeviceId, final int requestedNumberOfMessagesToFetch) {
    return loadTimer.record(() -> {
      final int numberOfMessagesToFetch = Math.min(requestedNumberOfMessagesToFetch, RESULT_SET_CHUNK_SIZE);
      final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid);
      final QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .consistentRead(true)
          .keyConditionExpression("#part = :part AND begins_with ( #sort , :sortprefix )")
          .expressionAttributeNames(Map.of(
              "#part", KEY_PARTITION,
              "#sort", KEY_SORT))
          .expressionAttributeValues(Map.of(
              ":part", partitionKey,
              ":sortprefix", convertDestinationDeviceIdToSortKeyPrefix(destinationDeviceId)))
          .limit(numberOfMessagesToFetch)
          .build();
      List<MessageProtos.Envelope> messageEntities = new ArrayList<>(numberOfMessagesToFetch);
      for (Map<String, AttributeValue> message : db().queryPaginator(queryRequest).items()) {
        try {
          messageEntities.add(convertItemToEnvelope(message));
        } catch (final InvalidProtocolBufferException e) {
          logger.error("Failed to parse envelope", e);
        }

        if (messageEntities.size() == numberOfMessagesToFetch) {
          // queryPaginator() uses limit() as the page size, not as an absolute limit
          // …but a page might be smaller than limit, because a page is capped at 1 MB
          break;
        }
      }
      return messageEntities;
    });
  }

  public Optional<MessageProtos.Envelope> deleteMessageByDestinationAndGuid(final UUID destinationAccountUuid,
      final UUID messageUuid) {
    return deleteByGuid.record(() -> {
      final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid);
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
      return deleteItemsMatchingQueryAndReturnFirstOneActuallyDeleted(partitionKey, queryRequest);
    });
  }

  public Optional<MessageProtos.Envelope> deleteMessage(final UUID destinationAccountUuid,
      final long destinationDeviceId, final UUID messageUuid, final long serverTimestamp) {
    return deleteByKey.record(() -> {
      final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid);
      final AttributeValue sortKey = convertSortKey(destinationDeviceId, serverTimestamp, messageUuid);
      DeleteItemRequest.Builder deleteItemRequest = DeleteItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(KEY_PARTITION, partitionKey, KEY_SORT, sortKey))
          .returnValues(ReturnValue.ALL_OLD);
      final DeleteItemResponse deleteItemResponse = db().deleteItem(deleteItemRequest.build());
      if (deleteItemResponse.attributes() != null && deleteItemResponse.attributes().containsKey(KEY_PARTITION)) {
        try {
          return Optional.of(convertItemToEnvelope(deleteItemResponse.attributes()));
        } catch (final InvalidProtocolBufferException e) {
          logger.error("Failed to parse envelope", e);
          return Optional.empty();
        }
      }

      return Optional.empty();
    });
  }

  @Nonnull
  private Optional<MessageProtos.Envelope> deleteItemsMatchingQueryAndReturnFirstOneActuallyDeleted(AttributeValue partitionKey, QueryRequest queryRequest) {
    Optional<MessageProtos.Envelope> result = Optional.empty();
    for (Map<String, AttributeValue> item : db().queryPaginator(queryRequest).items()) {
      final byte[] rangeKeyValue = item.get(KEY_SORT).b().asByteArray();
      DeleteItemRequest.Builder deleteItemRequest = DeleteItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(KEY_PARTITION, partitionKey, KEY_SORT, AttributeValues.fromByteArray(rangeKeyValue)));
      if (result.isEmpty()) {
        deleteItemRequest.returnValues(ReturnValue.ALL_OLD);
      }
      final DeleteItemResponse deleteItemResponse = db().deleteItem(deleteItemRequest.build());
      if (deleteItemResponse.attributes() != null && deleteItemResponse.attributes().containsKey(KEY_PARTITION)) {
        try {
          result = Optional.of(convertItemToEnvelope(deleteItemResponse.attributes()));
        } catch (final InvalidProtocolBufferException e) {
          logger.error("Failed to parse envelope", e);
        }
      }
    }
    return result;
  }

  public void deleteAllMessagesForAccount(final UUID destinationAccountUuid) {
    deleteByAccount.record(() -> {
      final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid);
      final QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .projectionExpression(KEY_SORT)
          .consistentRead(true)
          .keyConditionExpression("#part = :part")
          .expressionAttributeNames(Map.of("#part", KEY_PARTITION))
          .expressionAttributeValues(Map.of(":part", partitionKey))
          .build();
      deleteRowsMatchingQuery(partitionKey, queryRequest);
    });
  }

  public void deleteAllMessagesForDevice(final UUID destinationAccountUuid, final long destinationDeviceId) {
    deleteByDevice.record(() -> {
      final AttributeValue partitionKey = convertPartitionKey(destinationAccountUuid);
      final QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("#part = :part AND begins_with ( #sort , :sortprefix )")
          .expressionAttributeNames(Map.of(
              "#part", KEY_PARTITION,
              "#sort", KEY_SORT))
          .expressionAttributeValues(Map.of(
              ":part", partitionKey,
              ":sortprefix", convertDestinationDeviceIdToSortKeyPrefix(destinationDeviceId)))
          .projectionExpression(KEY_SORT)
          .consistentRead(true)
          .build();
      deleteRowsMatchingQuery(partitionKey, queryRequest);
    });
  }

  @VisibleForTesting
  static MessageProtos.Envelope convertItemToEnvelope(final Map<String, AttributeValue> item)
      throws InvalidProtocolBufferException {
    final MessageProtos.Envelope envelope;

    if (item.containsKey(KEY_ENVELOPE_BYTES)) {
      envelope = MessageProtos.Envelope.parseFrom(item.get(KEY_ENVELOPE_BYTES).b().asByteArray());

      GET_MESSAGE_WITH_ENVELOPE_COUNTER.increment();
    } else {
      final SortKey sortKey = convertSortKey(item.get(KEY_SORT).b().asByteArray());
      final UUID messageUuid = convertLocalIndexMessageUuidSortKey(item.get(LOCAL_INDEX_MESSAGE_UUID_KEY_SORT).b().asByteArray());
      final int type = AttributeValues.getInt(item, KEY_TYPE, 0);
      final long timestamp = AttributeValues.getLong(item, KEY_TIMESTAMP, 0L);
      final UUID sourceUuid = AttributeValues.getUUID(item, KEY_SOURCE_UUID, null);
      final int sourceDevice = AttributeValues.getInt(item, KEY_SOURCE_DEVICE, 0);
      final UUID destinationUuid = AttributeValues.getUUID(item, KEY_DESTINATION_UUID, null);
      final byte[] content = AttributeValues.getByteArray(item, KEY_CONTENT, null);
      final UUID updatedPni = AttributeValues.getUUID(item, KEY_UPDATED_PNI, null);

      envelope = new OutgoingMessageEntity(messageUuid, type, timestamp, sourceUuid, sourceDevice, destinationUuid,
          updatedPni, content, sortKey.getServerTimestamp(), true).toEnvelope();

      GET_MESSAGE_WITH_ATTRIBUTES_COUNTER.increment();
    }

    return envelope;
  }

  private void deleteRowsMatchingQuery(AttributeValue partitionKey, QueryRequest querySpec) {
    writeInBatches(db().queryPaginator(querySpec).items(), itemBatch -> deleteItems(partitionKey, itemBatch));
  }

  private void deleteItems(AttributeValue partitionKey, List<Map<String, AttributeValue>> items) {
    List<WriteRequest> deletes = items.stream()
        .map(item -> WriteRequest.builder()
            .deleteRequest(DeleteRequest.builder().key(Map.of(
                KEY_PARTITION, partitionKey,
                KEY_SORT, item.get(KEY_SORT))).build())
            .build())
        .collect(Collectors.toList());
    executeTableWriteItemsUntilComplete(Map.of(tableName, deletes));
  }

  private long getTtlForMessage(MessageProtos.Envelope message) {
    return message.getServerTimestamp() / 1000 + timeToLive.getSeconds();
  }

  private static AttributeValue convertPartitionKey(final UUID destinationAccountUuid) {
    return AttributeValues.fromUUID(destinationAccountUuid);
  }

  private static AttributeValue convertSortKey(final long destinationDeviceId, final long serverTimestamp, final UUID messageUuid) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[32]);
    byteBuffer.putLong(destinationDeviceId);
    byteBuffer.putLong(serverTimestamp);
    byteBuffer.putLong(messageUuid.getMostSignificantBits());
    byteBuffer.putLong(messageUuid.getLeastSignificantBits());
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  private static AttributeValue convertDestinationDeviceIdToSortKeyPrefix(final long destinationDeviceId) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[8]);
    byteBuffer.putLong(destinationDeviceId);
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  private static SortKey convertSortKey(final byte[] bytes) {
    if (bytes.length != 32) {
      throw new IllegalArgumentException("unexpected sort key byte length");
    }

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final long destinationDeviceId = byteBuffer.getLong();
    final long serverTimestamp = byteBuffer.getLong();
    final long mostSigBits = byteBuffer.getLong();
    final long leastSigBits = byteBuffer.getLong();
    return new SortKey(destinationDeviceId, serverTimestamp, new UUID(mostSigBits, leastSigBits));
  }

  private static AttributeValue convertLocalIndexMessageUuidSortKey(final UUID messageUuid) {
    return AttributeValues.fromUUID(messageUuid);
  }

  private static UUID convertLocalIndexMessageUuidSortKey(final byte[] bytes) {
    return convertUuidFromBytes(bytes, "local index message uuid sort key");
  }

  private static UUID convertUuidFromBytes(final byte[] bytes, final String name) {
    try {
      return UUIDUtil.fromBytes(bytes);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("unexpected " + name + " byte length; was " + bytes.length + " but expected 16");
    }
  }

  private static final class SortKey {
    private final long destinationDeviceId;
    private final long serverTimestamp;
    private final UUID messageUuid;

    public SortKey(long destinationDeviceId, long serverTimestamp, UUID messageUuid) {
      this.destinationDeviceId = destinationDeviceId;
      this.serverTimestamp = serverTimestamp;
      this.messageUuid = messageUuid;
    }

    public long getDestinationDeviceId() {
      return destinationDeviceId;
    }

    public long getServerTimestamp() {
      return serverTimestamp;
    }

    public UUID getMessageUuid() {
      return messageUuid;
    }
  }
}
