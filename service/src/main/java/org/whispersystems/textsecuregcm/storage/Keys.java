/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class Keys extends AbstractDynamoDbStore {

  private final String ecTableName;
  private final String pqTableName;
  private final String pqLastResortTableName;

  static final String KEY_ACCOUNT_UUID = "U";
  static final String KEY_DEVICE_ID_KEY_ID = "DK";
  static final String KEY_PUBLIC_KEY = "P";
  static final String KEY_SIGNATURE = "S";

  private static final Timer STORE_KEYS_TIMER = Metrics.timer(name(Keys.class, "storeKeys"));
  private static final Timer TAKE_KEY_FOR_DEVICE_TIMER = Metrics.timer(name(Keys.class, "takeKeyForDevice"));
  private static final Timer GET_KEY_COUNT_TIMER = Metrics.timer(name(Keys.class, "getKeyCount"));
  private static final Timer DELETE_KEYS_FOR_DEVICE_TIMER = Metrics.timer(name(Keys.class, "deleteKeysForDevice"));
  private static final Timer DELETE_KEYS_FOR_ACCOUNT_TIMER = Metrics.timer(name(Keys.class, "deleteKeysForAccount"));
  private static final DistributionSummary CONTESTED_KEY_DISTRIBUTION = Metrics.summary(name(Keys.class, "contestedKeys"));
  private static final DistributionSummary KEY_COUNT_DISTRIBUTION = Metrics.summary(name(Keys.class, "keyCount"));
  private static final Counter KEYS_EMPTY_TAKE_COUNTER = Metrics.counter(name(Keys.class, "takeKeyEmpty"));
  private static final Counter TOO_MANY_LAST_RESORT_KEYS_COUNTER = Metrics.counter(name(Keys.class, "tooManyLastResortKeys"));

  public Keys(
      final DynamoDbClient dynamoDB,
      final String ecTableName,
      final String pqTableName,
      final String pqLastResortTableName) {
    super(dynamoDB);
    this.ecTableName = ecTableName;
    this.pqTableName = pqTableName;
    this.pqLastResortTableName = pqLastResortTableName;
  }

  public void store(final UUID identifier, final long deviceId, final List<PreKey> keys) {
    store(identifier, deviceId, keys, null, null);
  }

  public void store(
      final UUID identifier, final long deviceId,
      @Nullable final List<PreKey> ecKeys,
      @Nullable final List<SignedPreKey> pqKeys,
      @Nullable final SignedPreKey pqLastResortKey) {
    Multimap<String, PreKey> keys = MultimapBuilder.hashKeys().arrayListValues().build();
    List<String> tablesToClear = new ArrayList<>();

    if (ecKeys != null && !ecKeys.isEmpty()) {
      keys.putAll(ecTableName, ecKeys);
      tablesToClear.add(ecTableName);
    }
    if (pqKeys != null && !pqKeys.isEmpty()) {
      keys.putAll(pqTableName, pqKeys);
      tablesToClear.add(pqTableName);
    }
    if (pqLastResortKey != null) {
      keys.put(pqLastResortTableName, pqLastResortKey);
      tablesToClear.add(pqLastResortTableName);
    }

    STORE_KEYS_TIMER.record(() -> {
      delete(tablesToClear, identifier, deviceId);

      writeInBatches(
          keys.entries(),
          batch -> {
            Multimap<String, WriteRequest> writes = batch.stream()
                .collect(
                    Multimaps.toMultimap(
                        Map.Entry<String, PreKey>::getKey,
                        entry -> WriteRequest.builder()
                            .putRequest(PutRequest.builder()
                                .item(getItemFromPreKey(identifier, deviceId, entry.getValue()))
                                .build())
                            .build(),
                        MultimapBuilder.hashKeys().arrayListValues()::build));
            executeTableWriteItemsUntilComplete(writes.asMap());
      });
    });
  }

  public void storePqLastResort(final UUID identifier, final Map<Long, SignedPreKey> keys) {
    final AttributeValue partitionKey = getPartitionKey(identifier);
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(pqLastResortTableName)
        .keyConditionExpression("#uuid = :uuid")
        .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(":uuid", partitionKey))
        .projectionExpression(KEY_DEVICE_ID_KEY_ID)
        .consistentRead(true)
        .build();

    final List<WriteRequest> writes = new ArrayList<>(2 * keys.size());
    final Map<Long, Map<String, AttributeValue>> newItems = keys.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> getItemFromPreKey(identifier, e.getKey(), e.getValue())));

    for (final Map<String, AttributeValue> item : db().query(queryRequest).items()) {
      final AttributeValue oldSortKey = item.get(KEY_DEVICE_ID_KEY_ID);
      final Long oldDeviceId = oldSortKey.b().asByteBuffer().getLong();
      if (newItems.containsKey(oldDeviceId)) {
        final Map<String, AttributeValue> replacement = newItems.get(oldDeviceId);
        if (!replacement.get(KEY_DEVICE_ID_KEY_ID).equals(oldSortKey)) {
          writes.add(WriteRequest.builder()
              .deleteRequest(DeleteRequest.builder()
                  .key(Map.of(
                          KEY_ACCOUNT_UUID, partitionKey,
                          KEY_DEVICE_ID_KEY_ID, oldSortKey))
                          .build())
              .build());
        }
      }
    }

    newItems.forEach((unusedKey, item) ->
        writes.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build()));

    executeTableWriteItemsUntilComplete(Map.of(pqLastResortTableName, writes));
  }

  public Optional<PreKey> takeEC(final UUID identifier, final long deviceId) {
    return take(ecTableName, identifier, deviceId);
  }

  public Optional<SignedPreKey> takePQ(final UUID identifier, final long deviceId) {
    return take(pqTableName, identifier, deviceId)
        .or(() -> getLastResort(identifier, deviceId))
        .map(pk -> (SignedPreKey) pk);
  }

  private Optional<PreKey> take(final String tableName, final UUID identifier, final long deviceId) {
    return TAKE_KEY_FOR_DEVICE_TIMER.record(() -> {
      final AttributeValue partitionKey = getPartitionKey(identifier);
      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
          .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
          .expressionAttributeValues(Map.of(
              ":uuid", partitionKey,
              ":sortprefix", getSortKeyPrefix(deviceId)))
          .projectionExpression(KEY_DEVICE_ID_KEY_ID)
          .consistentRead(false)
          .build();

      int contestedKeys = 0;

      try {
        QueryResponse response = db().query(queryRequest);
        for (Map<String, AttributeValue> candidate : response.items()) {
          DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
              .tableName(tableName)
              .key(Map.of(
                  KEY_ACCOUNT_UUID, partitionKey,
                  KEY_DEVICE_ID_KEY_ID, candidate.get(KEY_DEVICE_ID_KEY_ID)))
              .returnValues(ReturnValue.ALL_OLD)
              .build();
          DeleteItemResponse deleteItemResponse = db().deleteItem(deleteItemRequest);
          if (deleteItemResponse.hasAttributes()) {
            return Optional.of(getPreKeyFromItem(deleteItemResponse.attributes()));
          }

          contestedKeys++;
        }

        KEYS_EMPTY_TAKE_COUNTER.increment();
        return Optional.empty();
      } finally {
        CONTESTED_KEY_DISTRIBUTION.record(contestedKeys);
      }
    });
  }

  @VisibleForTesting
  Optional<PreKey> getLastResort(final UUID identifier, final long deviceId) {
      final AttributeValue partitionKey = getPartitionKey(identifier);
      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(pqLastResortTableName)
          .keyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
          .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
          .expressionAttributeValues(Map.of(
              ":uuid", partitionKey,
              ":sortprefix", getSortKeyPrefix(deviceId)))
          .consistentRead(false)
          .select(Select.ALL_ATTRIBUTES)
          .build();

      QueryResponse response = db().query(queryRequest);
      if (response.count() > 1) {
        TOO_MANY_LAST_RESORT_KEYS_COUNTER.increment();
      }
      return response.items().stream().findFirst().map(this::getPreKeyFromItem);
  }

  public List<Long> getPqEnabledDevices(final UUID identifier) {
    final AttributeValue partitionKey = getPartitionKey(identifier);
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(pqLastResortTableName)
        .keyConditionExpression("#uuid = :uuid")
        .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(":uuid", partitionKey))
        .projectionExpression(KEY_DEVICE_ID_KEY_ID)
        .consistentRead(false)
        .build();

    final QueryResponse response = db().query(queryRequest);
    return response.items().stream()
        .map(item -> item.get(KEY_DEVICE_ID_KEY_ID).b().asByteBuffer().getLong())
        .toList();
  }

  public int getEcCount(final UUID identifier, final long deviceId) {
    return getCount(ecTableName, identifier, deviceId);
  }

  public int getPqCount(final UUID identifier, final long deviceId) {
    return getCount(pqTableName, identifier, deviceId);
  }
  
  private int getCount(final String tableName, final UUID identifier, final long deviceId) {
    return GET_KEY_COUNT_TIMER.record(() -> {
      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
          .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
          .expressionAttributeValues(Map.of(
              ":uuid", getPartitionKey(identifier),
              ":sortprefix", getSortKeyPrefix(deviceId)))
          .select(Select.COUNT)
          .consistentRead(false)
          .build();

      int keyCount = 0;
      // This is very confusing, but does appear to be the intended behavior. See:
      //
      // - https://github.com/aws/aws-sdk-java/issues/693
      // - https://github.com/aws/aws-sdk-java/issues/915
      // - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Count
      for (final QueryResponse page : db().queryPaginator(queryRequest)) {
        keyCount += page.count();
      }
      KEY_COUNT_DISTRIBUTION.record(keyCount);
      return keyCount;
    });
  }

  public void delete(final UUID accountUuid) {
    DELETE_KEYS_FOR_ACCOUNT_TIMER.record(() -> {
      final QueryRequest queryRequest = QueryRequest.builder()
          .keyConditionExpression("#uuid = :uuid")
          .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
          .expressionAttributeValues(Map.of(
                  ":uuid", getPartitionKey(accountUuid)))
          .projectionExpression(KEY_DEVICE_ID_KEY_ID)
          .consistentRead(true)
          .build();

      deleteItemsForAccountMatchingQuery(List.of(ecTableName, pqTableName, pqLastResortTableName), accountUuid, queryRequest);
    });
  }

  public void delete(final UUID accountUuid, final long deviceId) {
    delete(List.of(ecTableName, pqTableName, pqLastResortTableName), accountUuid, deviceId);
  }

  private void delete(final List<String> tableNames, final UUID accountUuid, final long deviceId) {
    DELETE_KEYS_FOR_DEVICE_TIMER.record(() -> {
      final QueryRequest queryRequest = QueryRequest.builder()
          .keyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
          .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
          .expressionAttributeValues(Map.of(
                  ":uuid", getPartitionKey(accountUuid),
                  ":sortprefix", getSortKeyPrefix(deviceId)))
          .projectionExpression(KEY_DEVICE_ID_KEY_ID)
          .consistentRead(true)
          .build();

      deleteItemsForAccountMatchingQuery(tableNames, accountUuid, queryRequest);
    });
  }

  private void deleteItemsForAccountMatchingQuery(final List<String> tableNames, final UUID accountUuid, final QueryRequest querySpec) {
    final AttributeValue partitionKey = getPartitionKey(accountUuid);

    Multimap<String, Map<String, AttributeValue>> itemStream = tableNames.stream()
        .collect(
            Multimaps.flatteningToMultimap(
                Function.identity(),
                tableName ->
                    db().query(querySpec.toBuilder().tableName(tableName).build())
                        .items()
                        .stream(),
                MultimapBuilder.hashKeys(tableNames.size()).arrayListValues()::build));

    writeInBatches(
        itemStream.entries(),
        batch -> {
          Multimap<String, WriteRequest> deletes = batch.stream()
              .collect(Multimaps.toMultimap(
                  Map.Entry<String, Map<String, AttributeValue>>::getKey,
                  entry -> WriteRequest.builder()
                      .deleteRequest(DeleteRequest.builder()
                          .key(Map.of(
                              KEY_ACCOUNT_UUID, partitionKey,
                              KEY_DEVICE_ID_KEY_ID, entry.getValue().get(KEY_DEVICE_ID_KEY_ID)))
                          .build())
                      .build(),
                  MultimapBuilder.hashKeys(tableNames.size()).arrayListValues()::build));
          executeTableWriteItemsUntilComplete(deletes.asMap());
    });
  }

  private static AttributeValue getPartitionKey(final UUID accountUuid) {
    return AttributeValues.fromUUID(accountUuid);
  }

  private static AttributeValue getSortKey(final long deviceId, final long keyId) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
    byteBuffer.putLong(deviceId);
    byteBuffer.putLong(keyId);
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  @VisibleForTesting
  static AttributeValue getSortKeyPrefix(final long deviceId) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[8]);
    byteBuffer.putLong(deviceId);
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  private Map<String, AttributeValue> getItemFromPreKey(final UUID accountUuid, final long deviceId, final PreKey preKey) {
    if (preKey instanceof final SignedPreKey spk) {
      return Map.of(
          KEY_ACCOUNT_UUID, getPartitionKey(accountUuid),
          KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, spk.getKeyId()),
          KEY_PUBLIC_KEY, AttributeValues.fromString(spk.getPublicKey()),
          KEY_SIGNATURE, AttributeValues.fromString(spk.getSignature()));
    }
    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(accountUuid),
        KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, preKey.getKeyId()),
        KEY_PUBLIC_KEY, AttributeValues.fromString(preKey.getPublicKey()));
  }

  private PreKey getPreKeyFromItem(Map<String, AttributeValue> item) {
    final long keyId = item.get(KEY_DEVICE_ID_KEY_ID).b().asByteBuffer().getLong(8);
    if (item.containsKey(KEY_SIGNATURE)) {
      // All PQ prekeys are signed, and therefore have this attribute. Signed EC prekeys are stored
      // in the Accounts table, so EC prekeys retrieved by this class are never SignedPreKeys.
      return new SignedPreKey(keyId, item.get(KEY_PUBLIC_KEY).s(), item.get(KEY_SIGNATURE).s());
    }
    return new PreKey(keyId, item.get(KEY_PUBLIC_KEY).s());
  }
}
