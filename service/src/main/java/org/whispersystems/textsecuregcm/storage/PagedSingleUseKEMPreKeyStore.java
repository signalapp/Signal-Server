/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.signal.libsignal.protocol.InvalidKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

/**
 * @implNote This version of a {@link SingleUsePreKeyStore} store bundles prekeys into "pages", which are stored in on
 * an object store and referenced via dynamodb. Each device may only have a single active page at a time. Crashes or
 * errors may leave orphaned pages which are no longer referenced by the database. A background process must
 * periodically check for orphaned pages and remove them.
 * @see SingleUsePreKeyStore
 */
public class PagedSingleUseKEMPreKeyStore {

  private static final Logger log = LoggerFactory.getLogger(PagedSingleUseKEMPreKeyStore.class);

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final S3AsyncClient s3AsyncClient;
  private final String tableName;
  private final String bucketName;

  private final Timer getKeyCountTimer = Metrics.timer(name(getClass(), "getCount"));
  private final Timer storeKeyBatchTimer = Metrics.timer(name(getClass(), "storeKeyBatch"));
  private final Timer deleteForDeviceTimer = Metrics.timer(name(getClass(), "deleteForDevice"));
  private final Timer deleteForAccountTimer = Metrics.timer(name(getClass(), "deleteForAccount"));

  final DistributionSummary availableKeyCountDistributionSummary = DistributionSummary
      .builder(name(getClass(), "availableKeyCount"))
      .publishPercentileHistogram()
      .register(Metrics.globalRegistry);

  private final String takeKeyTimerName = name(getClass(), "takeKey");
  private static final String KEY_PRESENT_TAG_NAME = "keyPresent";

  static final String KEY_ACCOUNT_UUID = "U";
  static final String KEY_DEVICE_ID = "D";
  static final String ATTR_PAGE_ID = "ID";
  static final String ATTR_PAGE_IDX = "I";
  static final String ATTR_PAGE_NUM_KEYS = "N";
  static final String ATTR_PAGE_FORMAT_VERSION = "F";

  public PagedSingleUseKEMPreKeyStore(
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final S3AsyncClient s3AsyncClient,
      final String tableName,
      final String bucketName) {
    this.s3AsyncClient = s3AsyncClient;
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
    this.bucketName = bucketName;
  }

  /**
   * Stores a batch of single-use pre-keys for a specific device. All previously-stored keys for the device are cleared
   * before storing new keys.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId   the identifier for the device within the given account/identity
   * @param preKeys    a collection of single-use pre-keys to store for the target device
   * @return a future that completes when all previously-stored keys have been removed and the given collection of
   * pre-keys has been stored in its place
   */
  public CompletableFuture<Void> store(
      final UUID identifier, final byte deviceId, final List<KEMSignedPreKey> preKeys) {
    final Timer.Sample sample = Timer.start();

    final List<KEMSignedPreKey> sorted = preKeys.stream().sorted(Comparator.comparing(KEMSignedPreKey::keyId)).toList();

    final int bundleFormat = KEMPreKeyPage.FORMAT;
    final ByteBuffer bundle = KEMPreKeyPage.serialize(KEMPreKeyPage.FORMAT, sorted);

    // Write the bundle to S3, then update the database. Delete the S3 object that was in the database before. This can
    // leave orphans in S3 if we fail to update after writing to S3, or fail to delete the old page. However, it can
    // never leave a broken pointer in the database. To keep this invariant, we must make sure to generate a new
    // name for the page any time we were to retry this entire operation.
    return writeBundleToS3(identifier, deviceId, bundle)
        .thenCompose(pageId -> dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(identifier),
                KEY_DEVICE_ID, AttributeValues.fromInt(deviceId),
                ATTR_PAGE_ID, AttributeValues.fromUUID(pageId),
                ATTR_PAGE_IDX, AttributeValues.fromInt(0),
                ATTR_PAGE_NUM_KEYS, AttributeValues.fromInt(sorted.size()),
                ATTR_PAGE_FORMAT_VERSION, AttributeValues.fromInt(bundleFormat)
            ))
            .returnValues(ReturnValue.ALL_OLD)
            .build()))
        .thenCompose(response -> {
          if (response.hasAttributes()) {
            final UUID pageId = AttributeValues.getUUID(response.attributes(), ATTR_PAGE_ID, null);
            if (pageId == null) {
              log.error("Replaced record: {} with no pageId", response.attributes());
              return CompletableFuture.completedFuture(null);
            }
            return deleteBundleFromS3(identifier, deviceId, pageId);
          } else {
            return CompletableFuture.completedFuture(null);
          }
        })
        .whenComplete((result, error) -> sample.stop(storeKeyBatchTimer));
  }

  /**
   * Attempts to retrieve a single-use pre-key for a specific device. Keys may only be returned by this method at most
   * once; once the key is returned, it is removed from the key store and subsequent calls to this method will never
   * return the same key.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId   the identifier for the device within the given account/identity
   * @return a future that yields a single-use pre-key if one is available or empty if no single-use pre-keys are
   * available for the target device
   */
  public CompletableFuture<Optional<KEMSignedPreKey>> take(final UUID identifier, final byte deviceId) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.updateItem(UpdateItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(identifier),
                KEY_DEVICE_ID, AttributeValues.fromInt(deviceId)))
            .updateExpression("SET #index = #index + :one")
            .conditionExpression("#id = :id AND #index < #numkeys")
            .expressionAttributeNames(Map.of(
                "#id", KEY_ACCOUNT_UUID,
                "#index", ATTR_PAGE_IDX,
                "#numkeys", ATTR_PAGE_NUM_KEYS))
            .expressionAttributeValues(Map.of(
                ":one", AttributeValues.n(1),
                ":id", AttributeValues.fromUUID(identifier)))
            .returnValues(ReturnValue.ALL_OLD)
            .build())
        .thenCompose(updateItemResponse -> {
          if (!updateItemResponse.hasAttributes()) {
            throw new IllegalStateException("update succeeded but did not return an item");
          }

          final int index = AttributeValues.getInt(updateItemResponse.attributes(), ATTR_PAGE_IDX, -1);
          final UUID pageId = AttributeValues.getUUID(updateItemResponse.attributes(), ATTR_PAGE_ID, null);
          final int format = AttributeValues.getInt(updateItemResponse.attributes(), ATTR_PAGE_FORMAT_VERSION, -1);
          if (index < 0 || format < 0 || pageId == null) {
            throw new CompletionException(
                new IOException("unexpected page descriptor " + updateItemResponse.attributes()));
          }

          return readPreKeyAtIndexFromS3(identifier, deviceId, pageId, format, index).thenApply(Optional::of);
        })
        // If this check fails, it means that the item did not exist, or its index was already at the last key. Either
        // way, there are no keys left so we return empty
        .exceptionally(ExceptionUtils.exceptionallyHandler(
            ConditionalCheckFailedException.class,
            e -> Optional.empty()))
        .whenComplete((maybeKey, throwable) ->
            sample.stop(Metrics.timer(
                takeKeyTimerName,
                KEY_PRESENT_TAG_NAME, String.valueOf(maybeKey != null && maybeKey.isPresent()))));
  }

  /**
   * Returns the number of single-use pre-keys available for a given device.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId   the identifier for the device within the given account/identity
   * @return a future that yields the approximate number of single-use pre-keys currently available for the target
   * device
   */
  public CompletableFuture<Integer> getCount(final UUID identifier, final byte deviceId) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(identifier),
                KEY_DEVICE_ID, AttributeValues.fromInt(deviceId)))
            .consistentRead(true)
            .projectionExpression("#total, #index")
            .expressionAttributeNames(Map.of(
                "#total", ATTR_PAGE_NUM_KEYS,
                "#index", ATTR_PAGE_IDX))
            .build())
        .thenApply(getResponse -> {
          if (!getResponse.hasItem()) {
            return 0;
          }
          final int numKeys = AttributeValues.getInt(getResponse.item(), ATTR_PAGE_NUM_KEYS, -1);
          final int index = AttributeValues.getInt(getResponse.item(), ATTR_PAGE_IDX, -1);
          if (numKeys < 0 || index < 0 || index > numKeys) {
            log.error("unexpected index/length in page descriptor: {}", getResponse.item());
            return 0;
          }

          return numKeys - index;
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
   * @return a future that completes when all single-use pre-keys have been removed for all devices associated with the
   * given account/identity
   */
  public CompletableFuture<Void> delete(final UUID identifier) {
    final Timer.Sample sample = Timer.start();

    return deleteItems(identifier, Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("#uuid = :uuid")
            .projectionExpression("#uuid,#deviceid,#pageid")
            .expressionAttributeNames(Map.of(
                "#uuid", KEY_ACCOUNT_UUID,
                "#deviceid", KEY_DEVICE_ID,
                "#pageid", ATTR_PAGE_ID))
            .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(identifier)))
            .consistentRead(true)
            .build())
        .items()))
        .thenRun(() -> sample.stop(deleteForAccountTimer));
  }

  /**
   * Removes all single-use pre-keys for a specific device.
   *
   * @param identifier the identifier for the account/identity with which the target device is associated
   * @param deviceId   the identifier for the device within the given account/identity
   * @return a future that completes when all single-use pre-keys have been removed for the target device
   */
  public CompletableFuture<Void> delete(final UUID identifier, final byte deviceId) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbAsyncClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(identifier),
                KEY_DEVICE_ID, AttributeValues.fromInt(deviceId)))
            .consistentRead(true)
            .projectionExpression("#uuid,#deviceid,#pageid")
            .expressionAttributeNames(Map.of(
                "#uuid", KEY_ACCOUNT_UUID,
                "#deviceid", KEY_DEVICE_ID,
                "#pageid", ATTR_PAGE_ID))
            .build())
        .thenCompose(getItemResponse -> deleteItems(identifier, getItemResponse.hasItem()
            ? Flux.just(getItemResponse.item())
            : Flux.empty()))
        .thenRun(() -> sample.stop(deleteForDeviceTimer));
  }


  public Flux<DeviceKEMPreKeyPages> listStoredPages(int lookupConcurrency) {
    return Flux
        .from(s3AsyncClient.listObjectsV2Paginator(ListObjectsV2Request.builder()
            .bucket(bucketName)
            .build()))
        .flatMapIterable(ListObjectsV2Response::contents)
        .map(PagedSingleUseKEMPreKeyStore::parseS3Key)
        .bufferUntilChanged(Function.identity(), S3PageKey::fromSameDevice)
        .flatMapSequential(pages -> {
          final UUID identifier = pages.getFirst().identifier();
          final byte deviceId = pages.getFirst().deviceId();
          return Mono.fromCompletionStage(() -> dynamoDbAsyncClient.getItem(GetItemRequest.builder()
                  .tableName(tableName)
                  .key(Map.of(
                      KEY_ACCOUNT_UUID, AttributeValues.fromUUID(identifier),
                      KEY_DEVICE_ID, AttributeValues.fromInt(deviceId)))
                  // Make sure we get the most up to date pageId to minimize cases where we see a new page in S3 but
                  // view a stale dynamodb record
                  .consistentRead(true)
                  .projectionExpression("#uuid,#deviceid,#pageid")
                  .expressionAttributeNames(Map.of(
                      "#uuid", KEY_ACCOUNT_UUID,
                      "#deviceid", KEY_DEVICE_ID,
                      "#pageid", ATTR_PAGE_ID))
                  .build())
              .thenApply(getItemResponse -> new DeviceKEMPreKeyPages(
                  identifier,
                  deviceId,
                  Optional.ofNullable(AttributeValues.getUUID(getItemResponse.item(), ATTR_PAGE_ID, null)),
                  pages.stream().collect(Collectors.toMap(S3PageKey::pageId, S3PageKey::lastModified)))));
        }, lookupConcurrency);
  }

  private CompletableFuture<Void> deleteItems(final UUID identifier,
      final Flux<Map<String, AttributeValue>> items) {
    return items
        .flatMap(item -> {
          final UUID aci = AttributeValues.getUUID(item, KEY_ACCOUNT_UUID, null);
          final byte deviceId = (byte) AttributeValues.getInt(item, KEY_DEVICE_ID, -1);
          final UUID pageId = AttributeValues.getUUID(item, ATTR_PAGE_ID, null);
          if (aci == null || deviceId < 0 || pageId == null) {
            log.error("can't delete page from unexpected page descriptor {}", item);
          }
          return Mono.fromFuture(deleteBundleFromS3(aci, deviceId, pageId))
              .thenReturn(Map.of(
                  KEY_ACCOUNT_UUID, AttributeValues.fromUUID(identifier),
                  KEY_DEVICE_ID, AttributeValues.fromInt(deviceId)));
        })
        .flatMap(itemToDelete -> Mono.fromFuture(() -> dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(itemToDelete)
            .build())))
        .then()
        .toFuture()
        .thenRun(Util.NOOP);
  }

  private static String s3Key(final UUID identifier, final byte deviceId, final UUID pageId) {
    return String.format("%s/%s/%s", identifier, deviceId, pageId);
  }

  private record S3PageKey(UUID identifier, byte deviceId, UUID pageId, Instant lastModified) {

    boolean fromSameDevice(final S3PageKey other) {
      return deviceId == other.deviceId && identifier.equals(other.identifier);
    }
  }

  private static S3PageKey parseS3Key(final S3Object page) {
    try {
      final String[] parts = page.key().split("/", 3);
      if (parts.length != 3 || parts[2].contains("/")) {
        throw new IllegalArgumentException("wrong number of path components");
      }
      return new S3PageKey(
          UUID.fromString(parts[0]),
          Byte.parseByte(parts[1]),
          UUID.fromString(parts[2]), page.lastModified());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("invalid s3 page key: " + page.key(), e);
    }
  }


  private CompletableFuture<UUID> writeBundleToS3(final UUID identifier, final byte deviceId,
      final ByteBuffer bundle) {
    final UUID pageId = UUID.randomUUID();
    return s3AsyncClient.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key(identifier, deviceId, pageId)).build(),
            AsyncRequestBody.fromByteBuffer(bundle))
        .thenApply(ignoredResponse -> pageId);
  }

  CompletableFuture<Void> deleteBundleFromS3(final UUID identifier, final byte deviceId, final UUID pageId) {
    return s3AsyncClient.deleteObject(DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key(identifier, deviceId, pageId))
            .build())
        .thenRun(Util.NOOP);
  }

  private CompletableFuture<KEMSignedPreKey> readPreKeyAtIndexFromS3(
      final UUID identifier, final byte deviceId, final UUID pageId, final int format, final int index) {
    final KEMPreKeyPage.KeyLocation keyLocation = KEMPreKeyPage.keyLocation(format, index);
    return s3AsyncClient.getObject(GetObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Key(identifier, deviceId, pageId))
            // An RFC9110 range header, inclusive on both ends
            // https://www.rfc-editor.org/rfc/rfc9110.html#section-14.1.2
            .range("bytes=%s-%s".formatted(keyLocation.getStartInclusive(), keyLocation.getEndInclusive()))
            .build(), AsyncResponseTransformer.toBytes())
        .thenApply(bytes -> {
          final ByteBuffer serialized = bytes.asByteBuffer();
          if (serialized.remaining() != keyLocation.length()) {
            log.error("Unexpected ranged read response, requested {} got {} for offset {} in page {}",
                keyLocation.length(), serialized.remaining(), keyLocation, s3Key(identifier, deviceId, pageId));
            throw new CompletionException(new IOException("Invalid response to ranged read"));
          }
          try {
            return KEMPreKeyPage.deserializeKey(format, bytes.asByteBuffer());
          } catch (InvalidKeyException e) {
            throw new CompletionException(new IOException(e));
          }
        });
  }
}
