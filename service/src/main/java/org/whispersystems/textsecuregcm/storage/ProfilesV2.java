/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

public class ProfilesV2 {

  private static final String CONDITIONAL_CHECK_FAILED_CODE = "ConditionalCheckFailed";
  private static final String TRANSACTION_CONFLICT_CODE = "TransactionConflict";

  private final DynamoDbClient dynamoDbClient;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  // UUID of the account that owns this profile; hash key; byte array
  @VisibleForTesting
  static final String KEY_ACCOUNT_UUID = "U";

  // Version of this profile; sort key; byte array
  @VisibleForTesting
  static final String KEY_VERSION = "V";

  // Profile data (serialized ciphertext); byte array
  private static final String ATTR_DATA = "D";

  // SHA-256 hash of data attribute; byte array
  private static final String ATTR_DATA_HASH = "DH";

  // Commitment; byte array
  private static final String ATTR_COMMITMENT = "C";

  // Payment address; byte array
  private static final String ATTR_PAYMENT_ADDRESS = "P";

  // SHA-256 hash of payment address attribute; byte array
  private static final String ATTR_PAYMENT_ADDRESS_HASH = "PH";

  private static final Map<String, String> UPDATE_EXPRESSION_ATTRIBUTE_NAMES = Map.of(
      "#data", ATTR_DATA,
      "#dataHash", ATTR_DATA_HASH,
      "#commitment", ATTR_COMMITMENT,
      "#paymentAddress", ATTR_PAYMENT_ADDRESS,
      "#paymentAddressHash", ATTR_PAYMENT_ADDRESS_HASH);

  private static final Timer SET_PROFILES_TIMER = Metrics.timer(name(ProfilesV2.class, "set"));
  private static final Timer GET_PROFILE_TIMER = Metrics.timer(name(ProfilesV2.class, "get"));
  private static final String DELETE_PROFILES_TIMER_NAME = name(ProfilesV2.class, "delete");
  private static final String WRITE_CONFLICTS_COUNTER_NAME = name(ProfilesV2.class, "writeConflicts");

  private static final int COMMITMENT_LENGTH = 97;

  private static final int DELETE_MAX_CONCURRENCY = 32;

  public ProfilesV2(final DynamoDbClient dynamoDbClient,
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String tableName) {

    this.dynamoDbClient = dynamoDbClient;
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  /// Put a profile version with the given data
  ///
  /// During the migration from v1 &rarr; v2, both updates are executed transactionally
  ///
  /// @param uuid the account UUID
  /// @param version the profile version
  /// @param data serialized profile ciphertext
  /// @param commitment serialized profile key commitment (required for initial write, optional for updates)
  /// @param paymentAddress serialized payment address ciphertext (nullable)
  /// @param expectedCurrentDataHash the expected current data hash, used for conditional update (nullable for the initial write)
  /// @param profileV1TransactWriteItem transact write item for v1 profile during migration
  /// @throws WriteConflictException if the expected data hash does not match the stored data hash
  /// @throws IllegalArgumentException if commitment is null on initial write or commitment does not match on an update
  /// @throws IllegalStateException if commitment is missing after a write
  public void set(final UUID uuid,
      final byte[] version,
      final byte[] data,
      final byte[] dataHash,
      @Nullable final byte[] commitment,
      @Nullable final byte[] paymentAddress,
      @Nullable final byte[] paymentAddressHash,
      @Nullable final byte[] expectedCurrentDataHash,
      final TransactWriteItem profileV1TransactWriteItem) throws WriteConflictException {

    // Commitment is required for initial write (when expectedCurrentDataHash is null)
    if (expectedCurrentDataHash == null && commitment == null) {
      throw new IllegalArgumentException("Commitment is required for initial write");
    }

    final Timer.Sample sample = Timer.start();

    try {
      final List<TransactWriteItem> transactWriteItems = List.of(
          getTransactWriteItem(uuid, version, data, dataHash, commitment, paymentAddress, paymentAddressHash, expectedCurrentDataHash),
          profileV1TransactWriteItem);

      dynamoDbClient.transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(transactWriteItems)
          .build());
    } catch (TransactionCanceledException e) {

      if (e.hasCancellationReasons()) {

        // check for fields with conditional check failures
        if (e.cancellationReasons().getFirst() != null
            && e.cancellationReasons().getFirst().code().equals(CONDITIONAL_CHECK_FAILED_CODE)
            && e.cancellationReasons().getFirst().hasItem()) {

          final Map<String, AttributeValue> item = e.cancellationReasons().getFirst().item();

          final byte[] oldCommitment = AttributeValues.getByteArray(item, ATTR_COMMITMENT, null);

          if (oldCommitment != null && commitment != null && !Arrays.equals(oldCommitment, commitment)) {
            throw new IllegalArgumentException("Commitment is immutable");
          }

          if (oldCommitment == null && commitment == null) {
            throw new IllegalStateException("Commitment is absent from existing profile");
          }
        }

        final Optional<String> cancellationCode = e.cancellationReasons().stream()
            .map(CancellationReason::code)
            .filter(code -> CONDITIONAL_CHECK_FAILED_CODE.equals(code) || TRANSACTION_CONFLICT_CODE.equals(code))
            .findFirst();

        if (cancellationCode.isPresent()) {
          // any other conditional check failure is an expected data hash mismatch, which is a write conflict
          Metrics.counter(WRITE_CONFLICTS_COUNTER_NAME, "code", cancellationCode.get()).increment();
          throw new WriteConflictException(null);
        }
      }

      throw e;
    } finally {
      sample.stop(SET_PROFILES_TIMER);
    }
  }

  /// Get a [TransactWriteItem] for a profile update during the v2 migration
  ///
  /// @param uuid the account UUID
  /// @param version the profile version
  /// @param data the serialized profile ciphertext
  /// @param dataHash the serialized profile ciphertext hash
  /// @param commitment the profile key commitment (required for initial write, optional for updates)
  /// @param paymentAddress the payment address (nullable; will clear existing value for this version if null)
  /// @param paymentAddressHash the payment address hash (nullable; will clear existing value for this version if null)
  /// @param expectedCurrentDataHash the expected current data hash for conditional update (nullable for first write)
  /// @return a [TransactWriteItem] containing the Update operation
  @VisibleForTesting
  TransactWriteItem getTransactWriteItem(final UUID uuid,
      final byte[] version,
      final byte[] data,
      final byte[] dataHash,
      @Nullable final byte[] commitment,
      @Nullable final byte[] paymentAddress,
      @Nullable final byte[] paymentAddressHash,
      @Nullable final byte[] expectedCurrentDataHash) {

    return TransactWriteItem.builder()
        .update(Update.builder()
            .tableName(tableName)
            .key(buildPrimaryKey(uuid, version))
            .updateExpression(buildUpdateExpression(commitment != null, paymentAddress != null))
            .conditionExpression(buildConditionExpression(commitment != null, expectedCurrentDataHash != null))
            .expressionAttributeNames(UPDATE_EXPRESSION_ATTRIBUTE_NAMES)
            .expressionAttributeValues(buildUpdateExpressionAttributeValues(data, dataHash, commitment, paymentAddress, paymentAddressHash, expectedCurrentDataHash))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build();
  }

  private static Map<String, AttributeValue> buildPrimaryKey(final UUID uuid, final byte[] version) {
    return Map.of(
        KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
        KEY_VERSION, AttributeValues.fromByteArray(version));
  }

  @VisibleForTesting
  static String buildUpdateExpression(final boolean hasCommitment, final boolean hasPaymentAddress) {
    final StringBuilder updateExpressionBuilder = new StringBuilder("SET #data = :data, #dataHash = :dataHash");

    if (hasCommitment) {
      updateExpressionBuilder.append(", #commitment = if_not_exists(#commitment, :commitment)");
    }

    if (hasPaymentAddress) {
      updateExpressionBuilder.append(", #paymentAddress = :paymentAddress, #paymentAddressHash = :paymentAddressHash");
    } else {
      updateExpressionBuilder.append(" REMOVE #paymentAddress, #paymentAddressHash");
    }

    return updateExpressionBuilder.toString();
  }

  @VisibleForTesting
  static String buildConditionExpression(final boolean hasCommitment, final boolean hasExpectedCurrentDataHash) {

    final StringBuilder sb = new StringBuilder();

    if (hasCommitment) {
      sb.append("(attribute_not_exists(#commitment) OR #commitment = :commitment)");
    } else {
      sb.append("size(#commitment) = :commitment_length");
    }

    if (hasExpectedCurrentDataHash) {
      // If expected hash is provided, verify it matches the stored hash
      sb.append(" AND #dataHash = :expectedDataHash");
    } else {
      // Initial write: dataHash must not exist
      sb.append(" AND attribute_not_exists(#dataHash)");
    }

    return sb.toString();
  }

  @VisibleForTesting
  static Map<String, AttributeValue> buildUpdateExpressionAttributeValues(
      final byte[] data,
      final byte[] dataHash,
      @Nullable final byte[] commitment,
      @Nullable final byte[] paymentAddress,
      @Nullable final byte[] paymentAddressHash,
      @Nullable final byte[] expectedCurrentDataHash) {

    final Map<String, AttributeValue> expressionValues = new HashMap<>();

    expressionValues.put(":data", AttributeValues.fromByteArray(data));
    expressionValues.put(":dataHash", AttributeValues.fromByteArray(dataHash));

    if (commitment != null) {
      expressionValues.put(":commitment", AttributeValues.fromByteArray(commitment));
    } else {
      expressionValues.put(":commitment_length", AttributeValues.fromInt(COMMITMENT_LENGTH));
    }

    if (paymentAddress != null) {
      expressionValues.put(":paymentAddress", AttributeValues.fromByteArray(paymentAddress));
      expressionValues.put(":paymentAddressHash", AttributeValues.fromByteArray(paymentAddressHash));
    }

    if (expectedCurrentDataHash != null) {
      expressionValues.put(":expectedDataHash", AttributeValues.fromByteArray(expectedCurrentDataHash));
    }

    return expressionValues;
  }

  /// Fetch a profile version
  ///
  /// @param uuid the account UUID
  /// @param version the profile version
  /// @return the VersionedProfile, if found
  public Optional<VersionedProfile> get(final UUID uuid, final byte[] version) {
    return GET_PROFILE_TIMER.record(() -> {
      final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
          .tableName(tableName)
          .key(buildPrimaryKey(uuid, version))
          .consistentRead(true)
          .build());

      return response.hasItem() ? Optional.of(fromItem(response.item())) : Optional.empty();
    });
  }

  private static VersionedProfile fromItem(final Map<String, AttributeValue> item) {
    return new VersionedProfile(
        Objects.requireNonNull(AttributeValues.getByteArray(item, KEY_VERSION, null)),
        Objects.requireNonNull(AttributeValues.getByteArray(item, ATTR_DATA, null)),
        Objects.requireNonNull(AttributeValues.getByteArray(item, ATTR_DATA_HASH, null)),
        AttributeValues.getByteArray(item, ATTR_PAYMENT_ADDRESS, null),
        AttributeValues.getByteArray(item, ATTR_PAYMENT_ADDRESS_HASH, null),
        Objects.requireNonNull(AttributeValues.getByteArray(item, ATTR_COMMITMENT, null)));
  }

  /// Deletes all profile versions for the given UUID.
  ///
  /// @param uuid the account UUID
  /// @return a CompletableFuture that completes when all versions have been deleted
  public CompletableFuture<Void> deleteAll(final UUID uuid) {
    final Timer.Sample sample = Timer.start();

    final AttributeValue uuidAttributeValue = AttributeValues.fromUUID(uuid);

    return Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#uuid = :uuid")
                .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
                .expressionAttributeValues(Map.of(":uuid", uuidAttributeValue))
                .projectionExpression(KEY_VERSION)
                .consistentRead(true)
                .build())
            .items())
        .flatMap(item -> Mono.fromFuture(() -> dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_UUID, uuidAttributeValue,
                KEY_VERSION, item.get(KEY_VERSION)))
            .build())), DELETE_MAX_CONCURRENCY)
        .then()
        .doOnSuccess(_ -> sample.stop(Metrics.timer(DELETE_PROFILES_TIMER_NAME, "outcome", "success")))
        .doOnError(_ -> sample.stop(Metrics.timer(DELETE_PROFILES_TIMER_NAME, "outcome", "error")))
        .toFuture();
  }
}
