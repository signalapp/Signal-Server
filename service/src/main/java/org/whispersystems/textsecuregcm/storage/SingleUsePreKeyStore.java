/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;
import static org.whispersystems.textsecuregcm.storage.AbstractDynamoDbStore.DYNAMO_DB_MAX_BATCH_SIZE;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

/**
 * A single-use pre-key store stores single-use pre-keys of a specific type. Keys returned by a single-use pre-key
 * store's {@link #take(UUID, byte)} method are guaranteed to be returned exactly once, and repeated calls will never
 * yield the same key.
 * <p/>
 * Each {@link Account} may have one or more {@link Device devices}. Clients <em>should</em> regularly check their
 * supply of single-use pre-keys (see {@link #getCount(UUID, byte)}) and upload new keys when their supply runs low. In
 * the event that a party wants to begin a session with a device that has no single-use pre-keys remaining, that party
 * may fall back to using the device's repeated-use ("last-resort") signed pre-key instead.
 */
public abstract class SingleUsePreKeyStore<K extends PreKey<?>> {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  private final Timer getKeyCountTimer = Metrics.timer(name(getClass(), "getCount"));
  private final Timer storeKeyTimer = Metrics.timer(name(getClass(), "storeKey"));
  private final Timer storeKeyBatchTimer = Metrics.timer(name(getClass(), "storeKeyBatch"));
  private final Timer deleteForDeviceTimer = Metrics.timer(name(getClass(), "deleteForDevice"));
  private final Timer deleteForAccountTimer = Metrics.timer(name(getClass(), "deleteForAccount"));

  private final Counter noKeyCountAvailableCounter = Metrics.counter(name(getClass(), "noKeyCountAvailable"));

  final DistributionSummary keysConsideredForTakeDistributionSummary = DistributionSummary
      .builder(name(getClass(), "keysConsideredForTake"))
      .publishPercentiles(0.5, 0.75, 0.95, 0.99, 0.999)
      .distributionStatisticExpiry(Duration.ofMinutes(10))
      .register(Metrics.globalRegistry);

  final DistributionSummary availableKeyCountDistributionSummary = DistributionSummary
      .builder(name(getClass(), "availableKeyCount"))
      .publishPercentiles(0.5, 0.75, 0.95, 0.99, 0.999)
      .distributionStatisticExpiry(Duration.ofMinutes(10))
      .register(Metrics.globalRegistry);

  private final String takeKeyTimerName = name(getClass(), "takeKey");
  private static final String KEY_PRESENT_TAG_NAME = "keyPresent";

  static final String KEY_ACCOUNT_UUID = "U";
  static final String KEY_DEVICE_ID_KEY_ID = "DK";
  static final String ATTR_PUBLIC_KEY = "P";
  static final String ATTR_SIGNATURE = "S";
  static final String ATTR_REMAINING_KEYS = "R";

  protected SingleUsePreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  /**
   * Stores a batch of single-use pre-keys for a specific device. All previously-stored keys for the device are cleared
   * before storing new keys.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId the identifier for the device within the given account/identity
   * @param preKeys a collection of single-use pre-keys to store for the target device
   *
   * @return a future that completes when all previously-stored keys have been removed and the given collection of
   * pre-keys has been stored in its place
   */
  public CompletableFuture<Void> store(final UUID identifier, final byte deviceId, final List<K> preKeys) {
    final Timer.Sample sample = Timer.start();

    return Mono.fromFuture(() -> delete(identifier, deviceId))
        .thenMany(
            Flux.fromIterable(preKeys)
                .sort(Comparator.comparing(preKey -> preKey.keyId()))
                .zipWith(Flux.range(0, preKeys.size()).map(i -> preKeys.size() - i))
                .flatMap(preKeyAndRemainingCount -> Mono.fromFuture(() ->
                        store(identifier, deviceId, preKeyAndRemainingCount.getT1(), preKeyAndRemainingCount.getT2())),
                    DYNAMO_DB_MAX_BATCH_SIZE))
        .then()
        .toFuture()
        .thenRun(() -> sample.stop(storeKeyBatchTimer));
  }

  private CompletableFuture<Void> store(final UUID identifier, final byte deviceId, final K preKey, final int remainingKeys) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(getItemFromPreKey(identifier, deviceId, preKey, remainingKeys))
            .build())
        .thenRun(() -> sample.stop(storeKeyTimer));
  }

  /**
   * Attempts to retrieve a single-use pre-key for a specific device. Keys may only be returned by this method at most
   * once; once the key is returned, it is removed from the key store and subsequent calls to this method will never
   * return the same key.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId the identifier for the device within the given account/identity
   *
   * @return a future that yields a single-use pre-key if one is available or empty if no single-use pre-keys are
   * available for the target device
   */
  public CompletableFuture<Optional<K>> take(final UUID identifier, final byte deviceId) {
    final Timer.Sample sample = Timer.start();
    final AttributeValue partitionKey = getPartitionKey(identifier);
    final AtomicInteger keysConsidered = new AtomicInteger(0);

    return Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
                .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
                .expressionAttributeValues(Map.of(
                    ":uuid", partitionKey,
                    ":sortprefix", getSortKeyPrefix(deviceId)))
                .projectionExpression(KEY_DEVICE_ID_KEY_ID)
                .consistentRead(false)
                .limit(1)
                .build())
            .items())
        .map(item -> DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_UUID, partitionKey,
                KEY_DEVICE_ID_KEY_ID, item.get(KEY_DEVICE_ID_KEY_ID)))
            .returnValues(ReturnValue.ALL_OLD)
            .build())
        .flatMap(deleteItemRequest -> Mono.fromFuture(() -> dynamoDbAsyncClient.deleteItem(deleteItemRequest)), 1)
        .doOnNext(deleteItemResponse -> keysConsidered.incrementAndGet())
        .filter(DeleteItemResponse::hasAttributes)
        .next()
        .map(deleteItemResponse -> getPreKeyFromItem(deleteItemResponse.attributes()))
        .toFuture()
        .thenApply(Optional::ofNullable)
        .whenComplete((maybeKey, throwable) -> {
          sample.stop(Metrics.timer(takeKeyTimerName, KEY_PRESENT_TAG_NAME, String.valueOf(maybeKey != null && maybeKey.isPresent())));
          keysConsideredForTakeDistributionSummary.record(keysConsidered.get());
        });
  }

  /**
   * Estimates the number of single-use pre-keys available for a given device.

   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId the identifier for the device within the given account/identity

   * @return a future that yields the approximate number of single-use pre-keys currently available for the target
   * device
   */
  public CompletableFuture<Integer> getCount(final UUID identifier, final byte deviceId) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.query(QueryRequest.builder()
            .tableName(tableName)
            .consistentRead(false)
            .keyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
            .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
            .expressionAttributeValues(Map.of(
                ":uuid", getPartitionKey(identifier),
                ":sortprefix", getSortKeyPrefix(deviceId)))
            .projectionExpression(ATTR_REMAINING_KEYS)
            .limit(1)
            .build())
        .thenApply(response -> {
          if (response.count() > 0) {
            final Map<String, AttributeValue> item = response.items().getFirst();

            if (item.containsKey(ATTR_REMAINING_KEYS)) {
              return Integer.parseInt(item.get(ATTR_REMAINING_KEYS).n());
            } else {
              // Some legacy keys sets may not have pre-counted keys; in that case, we'll tell the owners of those key
              // sets that they have none remaining, prompting an upload of a fresh set that we'll pre-count. This has
              // no effect on consumers of keys, which will still be able to take keys if any are actually present.
              noKeyCountAvailableCounter.increment();
              return 0;
            }
          } else {
            return 0;
          }
        })
        .whenComplete((keyCount, throwable) -> {
          sample.stop(getKeyCountTimer);

          if (throwable == null && keyCount != null) {
            availableKeyCountDistributionSummary.record(keyCount);
          }
        });
  }

  /**
   * Removes all single-use pre-keys for all devices associated with the given account/identity.
   *
   * @param identifier the identifier for the account/identity for which to remove single-use pre-keys
   *
   * @return a future that completes when all single-use pre-keys have been removed for all devices associated with the
   * given account/identity
   */
  public CompletableFuture<Void> delete(final UUID identifier) {
    final Timer.Sample sample = Timer.start();

    return deleteItems(getPartitionKey(identifier), Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("#uuid = :uuid")
            .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
            .expressionAttributeValues(Map.of(":uuid", getPartitionKey(identifier)))
            .projectionExpression(KEY_DEVICE_ID_KEY_ID)
            .consistentRead(true)
            .build())
        .items()))
        .thenRun(() -> sample.stop(deleteForAccountTimer));
  }

  /**
   * Removes all single-use pre-keys for a specific device.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId the identifier for the device within the given account/identity

   * @return a future that completes when all single-use pre-keys have been removed for the target device
   */
  public CompletableFuture<Void> delete(final UUID identifier, final byte deviceId) {
    final Timer.Sample sample = Timer.start();

    return deleteItems(getPartitionKey(identifier), Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
            .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
            .expressionAttributeValues(Map.of(
                ":uuid", getPartitionKey(identifier),
                ":sortprefix", getSortKeyPrefix(deviceId)))
            .projectionExpression(KEY_DEVICE_ID_KEY_ID)
            .consistentRead(true)
            .build())
        .items()))
        .thenRun(() -> sample.stop(deleteForDeviceTimer));
  }

  private CompletableFuture<Void> deleteItems(final AttributeValue partitionKey, final Flux<Map<String, AttributeValue>> items) {
    return items
        .map(item -> DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_UUID, partitionKey,
                KEY_DEVICE_ID_KEY_ID, item.get(KEY_DEVICE_ID_KEY_ID)
            ))
            .build())
        .flatMap(deleteItemRequest -> Mono.fromFuture(() -> dynamoDbAsyncClient.deleteItem(deleteItemRequest)), DYNAMO_DB_MAX_BATCH_SIZE)
        .then()
        .toFuture()
        .thenRun(Util.NOOP);
  }

  protected static AttributeValue getPartitionKey(final UUID accountUuid) {
    return AttributeValues.fromUUID(accountUuid);
  }

  protected static AttributeValue getSortKey(final byte deviceId, final long keyId) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
    byteBuffer.putLong(deviceId);
    byteBuffer.putLong(keyId);
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  private static AttributeValue getSortKeyPrefix(final byte deviceId) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[8]);
    byteBuffer.putLong(deviceId);
    return AttributeValues.fromByteBuffer(byteBuffer.flip());
  }

  protected abstract Map<String, AttributeValue> getItemFromPreKey(final UUID identifier,
      final byte deviceId,
      final K preKey,
      final int remainingKeys);

  protected abstract K getPreKeyFromItem(final Map<String, AttributeValue> item);
}
