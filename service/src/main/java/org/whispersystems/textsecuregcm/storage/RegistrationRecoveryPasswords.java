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
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;

public class RegistrationRecoveryPasswords {

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
      final DynamoDbAsyncClient asyncClient,
      final Clock clock) {
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

  private static SaltedTokenHash saltedTokenHashFromItem(final Map<String, AttributeValue> item) {
    return new SaltedTokenHash(item.get(ATTR_HASH).s(), item.get(ATTR_SALT).s());
  }
}
