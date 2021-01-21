/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.api.QueryApi;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;
import static io.micrometer.core.instrument.Metrics.timer;

public class MessagesDynamoDb extends AbstractDynamoDbStore {

  private static final String KEY_PARTITION = "H";
  private static final String KEY_SORT = "S";
  private static final String LOCAL_INDEX_MESSAGE_UUID_NAME = "Message_UUID_Index";
  private static final String LOCAL_INDEX_MESSAGE_UUID_KEY_SORT = "U";

  private static final String KEY_TYPE = "T";
  private static final String KEY_RELAY = "R";
  private static final String KEY_TIMESTAMP = "TS";
  private static final String KEY_SOURCE = "SN";
  private static final String KEY_SOURCE_UUID = "SU";
  private static final String KEY_SOURCE_DEVICE = "SD";
  private static final String KEY_MESSAGE = "M";
  private static final String KEY_CONTENT = "C";
  private static final String KEY_TTL = "E";

  private final Timer storeTimer = timer(name(getClass(), "store"));
  private final Timer loadTimer = timer(name(getClass(), "load"));
  private final Timer deleteBySourceAndTimestamp = timer(name(getClass(), "delete", "sourceAndTimestamp"));
  private final Timer deleteByGuid = timer(name(getClass(), "delete", "guid"));
  private final Timer deleteByAccount = timer(name(getClass(), "delete", "account"));
  private final Timer deleteByDevice = timer(name(getClass(), "delete", "device"));

  private final String tableName;
  private final Duration timeToLive;

  public MessagesDynamoDb(DynamoDB dynamoDb, String tableName, Duration timeToLive) {
    super(dynamoDb);

    this.tableName = tableName;
    this.timeToLive = timeToLive;
  }

  public void store(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid, final long destinationDeviceId) {
    storeTimer.record(() -> writeInBatches(messages, (messageBatch) -> storeBatch(messageBatch, destinationAccountUuid, destinationDeviceId)));
  }

  private void storeBatch(final List<MessageProtos.Envelope> messages, final UUID destinationAccountUuid, final long destinationDeviceId) {
    if (messages.size() > DYNAMO_DB_MAX_BATCH_SIZE) {
      throw new IllegalArgumentException("Maximum batch size of " + DYNAMO_DB_MAX_BATCH_SIZE + " execeeded with " + messages.size() + " messages");
    }

    final byte[] partitionKey = convertPartitionKey(destinationAccountUuid);
    TableWriteItems items = new TableWriteItems(tableName);
    for (MessageProtos.Envelope message : messages) {
      final UUID messageUuid = UUID.fromString(message.getServerGuid());
      final Item item = new Item().withBinary(KEY_PARTITION, partitionKey)
                                  .withBinary(KEY_SORT, convertSortKey(destinationDeviceId, message.getServerTimestamp(), messageUuid))
                                  .withBinary(LOCAL_INDEX_MESSAGE_UUID_KEY_SORT, convertLocalIndexMessageUuidSortKey(messageUuid))
                                  .withInt(KEY_TYPE, message.getType().getNumber())
                                  .withLong(KEY_TIMESTAMP, message.getTimestamp())
                                  .withLong(KEY_TTL, getTtlForMessage(message));
      if (message.hasRelay() && message.getRelay().length() > 0) {
        item.withString(KEY_RELAY, message.getRelay());
      }
      if (message.hasSource()) {
        item.withString(KEY_SOURCE, message.getSource());
      }
      if (message.hasSourceUuid()) {
        item.withBinary(KEY_SOURCE_UUID, convertUuidToBytes(UUID.fromString(message.getSourceUuid())));
      }
      if (message.hasSourceDevice()) {
        item.withInt(KEY_SOURCE_DEVICE, message.getSourceDevice());
      }
      if (message.hasLegacyMessage()) {
        item.withBinary(KEY_MESSAGE, message.getLegacyMessage().toByteArray());
      }
      if (message.hasContent()) {
        item.withBinary(KEY_CONTENT, message.getContent().toByteArray());
      }
      items.addItemToPut(item);
    }

    executeTableWriteItemsUntilComplete(items);
  }

  public List<OutgoingMessageEntity> load(final UUID destinationAccountUuid, final long destinationDeviceId, final int requestedNumberOfMessagesToFetch) {
    return loadTimer.record(() -> {
      final int numberOfMessagesToFetch = Math.min(requestedNumberOfMessagesToFetch, RESULT_SET_CHUNK_SIZE);
      final byte[] partitionKey = convertPartitionKey(destinationAccountUuid);
      final QuerySpec querySpec = new QuerySpec().withConsistentRead(true)
                                                 .withKeyConditionExpression("#part = :part AND begins_with ( #sort , :sortprefix )")
                                                 .withNameMap(Map.of("#part", KEY_PARTITION,
                                                                     "#sort", KEY_SORT))
                                                 .withValueMap(Map.of(":part", partitionKey,
                                                                      ":sortprefix", convertDestinationDeviceIdToSortKeyPrefix(destinationDeviceId)))
                                                 .withMaxResultSize(numberOfMessagesToFetch);
      final Table table = getDynamoDb().getTable(tableName);
      List<OutgoingMessageEntity> messageEntities = new ArrayList<>(numberOfMessagesToFetch);
      for (Item message : table.query(querySpec)) {
        messageEntities.add(convertItemToOutgoingMessageEntity(message));
      }
      return messageEntities;
    });
  }

  public Optional<OutgoingMessageEntity> deleteMessageByDestinationAndSourceAndTimestamp(final UUID destinationAccountUuid, final long destinationDeviceId, final String source, final long timestamp) {
    return deleteBySourceAndTimestamp.record(() -> {
      if (StringUtils.isEmpty(source)) {
        throw new IllegalArgumentException("must specify a source");
      }

      final byte[] partitionKey = convertPartitionKey(destinationAccountUuid);
      final QuerySpec querySpec = new QuerySpec().withProjectionExpression(KEY_SORT)
                                                 .withConsistentRead(true)
                                                 .withKeyConditionExpression("#part = :part AND begins_with ( #sort , :sortprefix )")
                                                 .withFilterExpression("#source = :source AND #timestamp = :timestamp")
                                                 .withNameMap(Map.of("#part", KEY_PARTITION,
                                                                     "#sort", KEY_SORT,
                                                                     "#source", KEY_SOURCE,
                                                                     "#timestamp", KEY_TIMESTAMP))
                                                 .withValueMap(Map.of(":part", partitionKey,
                                                                      ":sortprefix", convertDestinationDeviceIdToSortKeyPrefix(destinationDeviceId),
                                                                      ":source", source,
                                                                      ":timestamp", timestamp));

      final Table table = getDynamoDb().getTable(tableName);
      return deleteItemsMatchingQueryAndReturnFirstOneActuallyDeleted(table, partitionKey, querySpec, table);
    });
  }

  public Optional<OutgoingMessageEntity> deleteMessageByDestinationAndGuid(final UUID destinationAccountUuid, final long destinationDeviceId, final UUID messageUuid) {
    return deleteByGuid.record(() -> {
      final byte[] partitionKey = convertPartitionKey(destinationAccountUuid);
      final QuerySpec querySpec = new QuerySpec().withProjectionExpression(KEY_SORT)
                                                 .withConsistentRead(true)
                                                 .withKeyConditionExpression("#part = :part AND #uuid = :uuid")
                                                 .withNameMap(Map.of("#part", KEY_PARTITION,
                                                                     "#uuid", LOCAL_INDEX_MESSAGE_UUID_KEY_SORT))
                                                 .withValueMap(Map.of(":part", partitionKey,
                                                                      ":uuid", convertLocalIndexMessageUuidSortKey(messageUuid)));
      final Table table = getDynamoDb().getTable(tableName);
      final Index index = table.getIndex(LOCAL_INDEX_MESSAGE_UUID_NAME);
      return deleteItemsMatchingQueryAndReturnFirstOneActuallyDeleted(table, partitionKey, querySpec, index);
    });
  }

  @Nonnull
  private Optional<OutgoingMessageEntity> deleteItemsMatchingQueryAndReturnFirstOneActuallyDeleted(Table table, byte[] partitionKey, QuerySpec querySpec, QueryApi queryApi) {
    Optional<OutgoingMessageEntity> result = Optional.empty();
    for (Item item : queryApi.query(querySpec)) {
      final byte[] rangeKeyValue = item.getBinary(KEY_SORT);
      DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(KEY_PARTITION, partitionKey, KEY_SORT, rangeKeyValue);
      if (result.isEmpty()) {
        deleteItemSpec.withReturnValues(ReturnValue.ALL_OLD);
      }
      final DeleteItemOutcome deleteItemOutcome = table.deleteItem(deleteItemSpec);
      if (deleteItemOutcome.getItem().hasAttribute(KEY_PARTITION)) {
        result = Optional.of(convertItemToOutgoingMessageEntity(deleteItemOutcome.getItem()));
      }
    }
    return result;
  }

  public void deleteAllMessagesForAccount(final UUID destinationAccountUuid) {
    deleteByAccount.record(() -> {
      final byte[] partitionKey = convertPartitionKey(destinationAccountUuid);
      final QuerySpec querySpec = new QuerySpec().withHashKey(KEY_PARTITION, partitionKey)
                                                 .withProjectionExpression(KEY_SORT)
                                                 .withConsistentRead(true);
      deleteRowsMatchingQuery(partitionKey, querySpec);
    });
  }

  public void deleteAllMessagesForDevice(final UUID destinationAccountUuid, final long destinationDeviceId) {
    deleteByDevice.record(() -> {
      final byte[] partitionKey = convertPartitionKey(destinationAccountUuid);
      final QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#part = :part AND begins_with ( #sort , :sortprefix )")
                                                 .withNameMap(Map.of("#part", KEY_PARTITION,
                                                                     "#sort", KEY_SORT))
                                                 .withValueMap(Map.of(":part", partitionKey,
                                                                      ":sortprefix", convertDestinationDeviceIdToSortKeyPrefix(destinationDeviceId)))
                                                 .withProjectionExpression(KEY_SORT)
                                                 .withConsistentRead(true);
      deleteRowsMatchingQuery(partitionKey, querySpec);
    });
  }

  private OutgoingMessageEntity convertItemToOutgoingMessageEntity(Item message) {
    final SortKey sortKey = convertSortKey(message.getBinary(KEY_SORT));
    final UUID messageUuid = convertLocalIndexMessageUuidSortKey(message.getBinary(LOCAL_INDEX_MESSAGE_UUID_KEY_SORT));
    final int type = message.getInt(KEY_TYPE);
    final String relay = message.getString(KEY_RELAY);
    final long timestamp = message.getLong(KEY_TIMESTAMP);
    final String source = message.getString(KEY_SOURCE);
    final UUID sourceUuid = message.hasAttribute(KEY_SOURCE_UUID) ? convertUuidFromBytes(message.getBinary(KEY_SOURCE_UUID), "message source uuid") : null;
    final int sourceDevice = message.hasAttribute(KEY_SOURCE_DEVICE) ? message.getInt(KEY_SOURCE_DEVICE) : 0;
    final byte[] messageBytes = message.getBinary(KEY_MESSAGE);
    final byte[] content = message.getBinary(KEY_CONTENT);
    return new OutgoingMessageEntity(-1L, false, messageUuid, type, relay, timestamp, source, sourceUuid, sourceDevice, messageBytes, content, sortKey.getServerTimestamp());
  }

  private void deleteRowsMatchingQuery(byte[] partitionKey, QuerySpec querySpec) {
    final Table table = getDynamoDb().getTable(tableName);
    writeInBatches(table.query(querySpec), (itemBatch) -> deleteItems(partitionKey, itemBatch));
  }

  private void deleteItems(byte[] partitionKey, List<Item> items) {
    final TableWriteItems tableWriteItems = new TableWriteItems(tableName);
    items.stream().map(item -> new PrimaryKey(KEY_PARTITION, partitionKey, KEY_SORT, item.getBinary(KEY_SORT))).forEach(tableWriteItems::addPrimaryKeyToDelete);
    executeTableWriteItemsUntilComplete(tableWriteItems);
  }

  private long getTtlForMessage(MessageProtos.Envelope message) {
    return message.getServerTimestamp() / 1000 + timeToLive.getSeconds();
  }

  private static byte[] convertPartitionKey(final UUID destinationAccountUuid) {
    return convertUuidToBytes(destinationAccountUuid);
  }

  private static byte[] convertSortKey(final long destinationDeviceId, final long serverTimestamp, final UUID messageUuid) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[32]);
    byteBuffer.putLong(destinationDeviceId);
    byteBuffer.putLong(serverTimestamp);
    byteBuffer.putLong(messageUuid.getMostSignificantBits());
    byteBuffer.putLong(messageUuid.getLeastSignificantBits());
    return byteBuffer.array();
  }

  private static byte[] convertDestinationDeviceIdToSortKeyPrefix(final long destinationDeviceId) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[8]);
    byteBuffer.putLong(destinationDeviceId);
    return byteBuffer.array();
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

  private static byte[] convertLocalIndexMessageUuidSortKey(final UUID messageUuid) {
    return convertUuidToBytes(messageUuid);
  }

  private static UUID convertLocalIndexMessageUuidSortKey(final byte[] bytes) {
    return convertUuidFromBytes(bytes, "local index message uuid sort key");
  }

  private static byte[] convertUuidToBytes(final UUID uuid) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());
    return byteBuffer.array();
  }

  private static UUID convertUuidFromBytes(final byte[] bytes, final String name) {
    if (bytes.length != 16) {
      throw new IllegalArgumentException("unexpected " + name + " byte length; was " + bytes.length + " but expected 16");
    }

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final long mostSigBits = byteBuffer.getLong();
    final long leastSigBits = byteBuffer.getLong();
    return new UUID(mostSigBits, leastSigBits);
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
