/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;

/**
 * A repeated-use signed pre-key store manages storage for pre-keys that may be used more than once. Generally, these
 * are considered "last resort" keys and should only be used when a device's supply of single-use pre-keys has been
 * exhausted.
 * <p/>
 * Each {@link Account} may have one or more {@link Device devices}. Each "active" (i.e. those that have completed
 * provisioning and are capable of sending and receiving messages) must have exactly one "last resort" pre-key.
 */
public abstract class RepeatedUseSignedPreKeyStore<K extends SignedPreKey<?>> {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  static final String KEY_ACCOUNT_UUID = "U";
  static final String KEY_DEVICE_ID = "D";
  static final String ATTR_KEY_ID = "I";
  static final String ATTR_PUBLIC_KEY = "P";
  static final String ATTR_SIGNATURE = "S";

  private final Timer storeSingleKeyTimer = Metrics.timer(MetricsUtil.name(getClass(), "storeSingleKey"));
  private final Timer storeKeyBatchTimer = Metrics.timer(MetricsUtil.name(getClass(), "storeKeyBatch"));
  private final Timer deleteForDeviceTimer = Metrics.timer(MetricsUtil.name(getClass(), "deleteForDevice"));
  private final Timer deleteForAccountTimer = Metrics.timer(MetricsUtil.name(getClass(), "deleteForAccount"));

  private final String findKeyTimerName = MetricsUtil.name(getClass(), "findKey");

  public RepeatedUseSignedPreKeyStore(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  /**
   * Stores a repeated-use pre-key for a specific device, displacing any previously-stored repeated-use pre-key for that
   * device.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId the identifier for the device within the given account/identity
   * @param signedPreKey the key to store for the target device
   *
   * @return a future that completes once the key has been stored
   */
  public CompletableFuture<Void> store(final UUID identifier, final long deviceId, final K signedPreKey) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(getItemFromPreKey(identifier, deviceId, signedPreKey))
            .build())
        .thenRun(() -> sample.stop(storeSingleKeyTimer));
  }

  /**
   * Stores repeated-use pre-keys for a collection of devices associated with a single account/identity, displacing any
   * previously-stored repeated-use pre-keys for the targeted devices. Note that this method is transactional; either
   * all keys will be stored or none will.
   *
   * @param identifier the identifier for the account/identity with which the target devices are associated
   * @param signedPreKeysByDeviceId a map of device identifiers to pre-keys
   *
   * @return a future that completes once all keys have been stored
   */
  public CompletableFuture<Void> store(final UUID identifier, final Map<Long, K> signedPreKeysByDeviceId) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(signedPreKeysByDeviceId.entrySet().stream()
                .map(entry -> {
                  final long deviceId = entry.getKey();
                  final K signedPreKey = entry.getValue();

                  return TransactWriteItem.builder()
                      .put(Put.builder()
                          .tableName(tableName)
                          .item(getItemFromPreKey(identifier, deviceId, signedPreKey))
                          .build())
                      .build();
                })
                .toList())
        .build())
        .thenRun(() -> sample.stop(storeKeyBatchTimer));
  }

  /**
   * Finds a repeated-use pre-key for a specific device.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId the identifier for the device within the given account/identity
   *
   * @return a future that yields an optional signed pre-key if one is available for the target device or empty if no
   * key could be found for the target device
   */
  public CompletableFuture<Optional<K>> find(final UUID identifier, final long deviceId) {
    final Timer.Sample sample = Timer.start();

    final CompletableFuture<Optional<K>> findFuture = dynamoDbAsyncClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(getPrimaryKey(identifier, deviceId))
            .consistentRead(true)
            .build())
        .thenApply(response -> response.hasItem() ? Optional.of(getPreKeyFromItem(response.item())) : Optional.empty());

    findFuture.whenComplete((maybeSignedPreKey, throwable) ->
        sample.stop(Metrics.timer(findKeyTimerName,
            "keyPresent", String.valueOf(maybeSignedPreKey != null && maybeSignedPreKey.isPresent()))));

    return findFuture;
  }

  /**
   * Clears all repeated-use pre-keys associated with the given account/identity.
   *
   * @param identifier the identifier for the account/identity for which to clear repeated-use pre-keys
   *
   * @return a future that completes once repeated-use pre-keys have been cleared from all devices associated with the
   * target account/identity
   */
  public CompletableFuture<Void> delete(final UUID identifier) {
    final Timer.Sample sample = Timer.start();

    return getDeviceIdsWithKeys(identifier)
        .map(deviceId -> DeleteItemRequest.builder()
            .tableName(tableName)
            .key(getPrimaryKey(identifier, deviceId))
            .build())
        .flatMap(deleteItemRequest -> Mono.fromFuture(() -> dynamoDbAsyncClient.deleteItem(deleteItemRequest)))
        // Idiom: wait for everything to finish, but discard the results
        .reduce(0, (a, b) -> 0)
        .toFuture()
        .thenRun(() -> sample.stop(deleteForAccountTimer));
  }

  /**
   * Removes the repeated-use pre-key associated with a specific device.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId the identifier for the device within the given account/identity
   *
   * @return a future that completes once the repeated-use pre-key has been removed from the target device
   */
  public CompletableFuture<Void> delete(final UUID identifier, final long deviceId) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(getPrimaryKey(identifier, deviceId))
        .build())
        .thenRun(() -> sample.stop(deleteForDeviceTimer));
  }

  public Flux<Long> getDeviceIdsWithKeys(final UUID identifier) {
    return Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("#uuid = :uuid")
            .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
            .expressionAttributeValues(Map.of(
                ":uuid", getPartitionKey(identifier)))
            .projectionExpression(KEY_DEVICE_ID)
            .consistentRead(true)
            .build())
        .items())
        .map(item -> Long.parseLong(item.get(KEY_DEVICE_ID).n()));
  }

  protected static Map<String, AttributeValue> getPrimaryKey(final UUID identifier, final long deviceId) {
    return Map.of(
        KEY_ACCOUNT_UUID, getPartitionKey(identifier),
        KEY_DEVICE_ID, getSortKey(deviceId));
  }

  protected static AttributeValue getPartitionKey(final UUID accountUuid) {
    return AttributeValues.fromUUID(accountUuid);
  }

  protected static AttributeValue getSortKey(final long deviceId) {
    return AttributeValues.fromLong(deviceId);
  }

  protected abstract Map<String, AttributeValue> getItemFromPreKey(final UUID accountUuid, final long deviceId, final K signedPreKey);

  protected abstract K getPreKeyFromItem(final Map<String, AttributeValue> item);
}
