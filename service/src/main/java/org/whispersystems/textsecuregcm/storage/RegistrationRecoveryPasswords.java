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
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

public class RegistrationRecoveryPasswords {

  // For historical reasons, we record the PNI as a UUID string rather than a compact byte array
  static final String KEY_PNI = "P";
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

  public CompletableFuture<Optional<SaltedTokenHash>> lookup(final UUID phoneNumberIdentifier) {
    return asyncClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_PNI, AttributeValues.fromString(phoneNumberIdentifier.toString())))
            .consistentRead(true)
            .build())
        .thenApply(getItemResponse -> Optional.ofNullable(getItemResponse.item())
            .filter(item -> item.containsKey(ATTR_SALT))
            .filter(item -> item.containsKey(ATTR_HASH))
            .map(RegistrationRecoveryPasswords::saltedTokenHashFromItem));
  }

  ///  Add a PNI -> RRP mapping, or replace the current one if it already exists
  ///
  /// @param phoneNumberIdentifier The PNI to associate the salted RRP with
  /// @param data The salted registration recovery password
  /// @return true if a new mapping was added, false if an existing mapping was updated
  public CompletableFuture<Boolean> addOrReplace(final UUID phoneNumberIdentifier, final SaltedTokenHash data) {
    final long expirationSeconds = expirationSeconds();

    return asyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .returnValues(ReturnValue.ALL_OLD)
            .item(Map.of(
                KEY_PNI, AttributeValues.fromString(phoneNumberIdentifier.toString()),
                ATTR_EXP, AttributeValues.fromLong(expirationSeconds),
                ATTR_SALT, AttributeValues.fromString(data.salt()),
                ATTR_HASH, AttributeValues.fromString(data.hash())))
            .build())
        .thenApply(response -> response.attributes() == null || response.attributes().isEmpty());
  }

  ///  Remove the entry associated with the provided PNI
  ///
  /// @return true if an entry was removed, false if no entry existed
  public CompletableFuture<Boolean> removeEntry(final UUID phoneNumberIdentifier) {
    return asyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .returnValues(ReturnValue.ALL_OLD)
            .key(Map.of(KEY_PNI, AttributeValues.fromString(phoneNumberIdentifier.toString())))
            .build())
        .thenApply(response -> response.attributes() != null && !response.attributes().isEmpty());
  }

  @VisibleForTesting
  long expirationSeconds() {
    return clock.instant().plus(expiration).getEpochSecond();
  }

  private static SaltedTokenHash saltedTokenHashFromItem(final Map<String, AttributeValue> item) {
    return new SaltedTokenHash(item.get(ATTR_HASH).s(), item.get(ATTR_SALT).s());
  }
}
