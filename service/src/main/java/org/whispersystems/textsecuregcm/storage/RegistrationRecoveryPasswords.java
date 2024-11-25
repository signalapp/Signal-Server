/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

public class RegistrationRecoveryPasswords extends AbstractDynamoDbStore {

  // As a temporary transitional measure, this can be either a string representation of an E164-formatted phone number
  // or a UUID (PNI) string
  static final String KEY_E164 = "P";
  static final String ATTR_EXP = "E";
  static final String ATTR_SALT = "S";
  static final String ATTR_HASH = "H";

  private final String tableName;

  private final Duration expiration;

  private final DynamoDbAsyncClient asyncClient;

  private final Clock clock;

  public RegistrationRecoveryPasswords(
      final String tableName,
      final Duration expiration,
      final DynamoDbClient dynamoDbClient,
      final DynamoDbAsyncClient asyncClient) {
    this(tableName, expiration, dynamoDbClient, asyncClient, Clock.systemUTC());
  }

  RegistrationRecoveryPasswords(
      final String tableName,
      final Duration expiration,
      final DynamoDbClient dynamoDbClient,
      final DynamoDbAsyncClient asyncClient,
      final Clock clock) {
    super(dynamoDbClient);
    this.tableName = requireNonNull(tableName);
    this.expiration = requireNonNull(expiration);
    this.asyncClient = requireNonNull(asyncClient);
    this.clock = requireNonNull(clock);
  }

  public CompletableFuture<Optional<SaltedTokenHash>> lookup(final String number) {
    return asyncClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_E164, AttributeValues.fromString(number)))
            .consistentRead(true)
        .build())
        .thenApply(getItemResponse -> Optional.ofNullable(getItemResponse.item())
            .filter(item -> item.containsKey(ATTR_SALT))
            .filter(item -> item.containsKey(ATTR_HASH))
            .map(RegistrationRecoveryPasswords::saltedTokenHashFromItem));
  }

  public CompletableFuture<Optional<SaltedTokenHash>> lookup(final UUID phoneNumberIdentifier) {
    return lookup(phoneNumberIdentifier.toString());
  }

  public CompletableFuture<Void> addOrReplace(final String number, final UUID phoneNumberIdentifier, final SaltedTokenHash data) {
    final long expirationSeconds = expirationSeconds();

    return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(
                buildPutRecoveryPasswordWriteItem(number, expirationSeconds, data.salt(), data.hash()),
                buildPutRecoveryPasswordWriteItem(phoneNumberIdentifier.toString(), expirationSeconds, data.salt(), data.hash()))
        .build())
        .thenRun(Util.NOOP);
  }

  private TransactWriteItem buildPutRecoveryPasswordWriteItem(final String key,
      final long expirationSeconds,
      final String salt,
      final String hash) {

    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_E164, AttributeValues.fromString(key),
                ATTR_EXP, AttributeValues.fromLong(expirationSeconds),
                ATTR_SALT, AttributeValues.fromString(salt),
                ATTR_HASH, AttributeValues.fromString(hash)))
            .build())
        .build();
  }

  public CompletableFuture<Void> removeEntry(final String number, final UUID phoneNumberIdentifier) {
    return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(
            buildDeleteRecoveryPasswordWriteItem(number),
            buildDeleteRecoveryPasswordWriteItem(phoneNumberIdentifier.toString()))
        .build())
        .thenRun(Util.NOOP);
  }

  private TransactWriteItem buildDeleteRecoveryPasswordWriteItem(final String key) {
    return TransactWriteItem.builder()
        .delete(Delete.builder()
            .tableName(tableName)
            .key(Map.of(KEY_E164, AttributeValues.fromString(key)))
            .build())
        .build();
  }

  @VisibleForTesting
  long expirationSeconds() {
    return clock.instant().plus(expiration).getEpochSecond();
  }

  public Flux<Tuple3<String, SaltedTokenHash, Long>> getE164AssociatedRegistrationRecoveryPasswords(final int segments, final Scheduler scheduler) {
    if (segments < 1) {
      throw new IllegalArgumentException("Total number of segments must be positive");
    }

    return Flux.range(0, segments)
        .parallel()
        .runOn(scheduler)
        .flatMap(segment -> asyncClient.scanPaginator(ScanRequest.builder()
                .tableName(tableName)
                .consistentRead(true)
                .segment(segment)
                .totalSegments(segments)
                .filterExpression("begins_with(#key, :e164Prefix)")
                .expressionAttributeNames(Map.of("#key", KEY_E164))
                .expressionAttributeValues(Map.of(":e164Prefix", AttributeValue.fromS("+")))
                .build())
            .items()
            .map(item -> Tuples.of(item.get(KEY_E164).s(), saltedTokenHashFromItem(item), Long.parseLong(item.get(ATTR_EXP).n()))))
        .sequential();
  }

  public CompletableFuture<Boolean> insertPniRecord(final String phoneNumber,
      final UUID phoneNumberIdentifier,
      final SaltedTokenHash saltedTokenHash,
      final long expirationSeconds) {

    // We try to write both the old and new record inside a transaction, but with different conditions. For the
    // E164-based record, we insist that the record be entirely unchanged. This prevents us from writing an out-of-sync
    // record if we read one thing in the `Scan` pass, but then somebody updated the record before we tried to write
    // the PNI-based record. We refresh and retry if this happens.
    //
    // For the PNI-based record, we only want to write the record if one doesn't already exist for the given PNI. If one
    // already exists, we'll just leave it alone.
    return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(
            TransactWriteItem.builder()
                .put(Put.builder()
                    .tableName(tableName)
                    .item(Map.of(
                        KEY_E164, AttributeValues.fromString(phoneNumber),
                        ATTR_EXP, AttributeValues.fromLong(expirationSeconds),
                        ATTR_SALT, AttributeValues.fromString(saltedTokenHash.salt()),
                        ATTR_HASH, AttributeValues.fromString(saltedTokenHash.hash())))
                    .conditionExpression("#key = :key AND #expiration = :expiration AND #salt = :salt AND #hash = :hash")
                    .expressionAttributeNames(Map.of(
                        "#key", KEY_E164,
                        "#expiration", ATTR_EXP,
                        "#salt", ATTR_SALT,
                        "#hash", ATTR_HASH))
                    .expressionAttributeValues(Map.of(
                        ":key", AttributeValues.fromString(phoneNumber),
                        ":expiration", AttributeValues.fromLong(expirationSeconds),
                        ":salt", AttributeValues.fromString(saltedTokenHash.salt()),
                        ":hash", AttributeValues.fromString(saltedTokenHash.hash())))
                    .build())
                .build(),

            TransactWriteItem.builder()
                .put(Put.builder()
                    .tableName(tableName)
                    .item(Map.of(
                        KEY_E164, AttributeValues.fromString(phoneNumberIdentifier.toString()),
                        ATTR_EXP, AttributeValues.fromLong(expirationSeconds),
                        ATTR_SALT, AttributeValues.fromString(saltedTokenHash.salt()),
                        ATTR_HASH, AttributeValues.fromString(saltedTokenHash.hash())))
                    .conditionExpression("attribute_not_exists(#key)")
                    .expressionAttributeNames(Map.of("#key", KEY_E164))
                    .build())
                .build())
            .build())
        .thenApply(ignored -> true)
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException transactionCanceledException) {
            if ("ConditionalCheckFailed".equals(transactionCanceledException.cancellationReasons().get(1).code())) {
              // A PNI-associated record has already been stored; we can just treat this as success
              return false;
            }

            if ("ConditionalCheckFailed".equals(transactionCanceledException.cancellationReasons().get(0).code())) {
              // No PNI-associated record is present, but the original record has changed
              throw new ContestedOptimisticLockException();
            }
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }

  private static SaltedTokenHash saltedTokenHashFromItem(final Map<String, AttributeValue> item) {
    return new SaltedTokenHash(item.get(ATTR_HASH).s(), item.get(ATTR_SALT).s());
  }
}
