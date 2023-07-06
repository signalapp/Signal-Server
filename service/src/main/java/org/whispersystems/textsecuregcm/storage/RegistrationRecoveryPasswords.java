/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class RegistrationRecoveryPasswords extends AbstractDynamoDbStore {

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
        .thenApply(getItemResponse -> {
          final Map<String, AttributeValue> item = getItemResponse.item();
          if (item == null || !item.containsKey(ATTR_SALT) || !item.containsKey(ATTR_HASH)) {
            return Optional.empty();
          }
          final String salt = item.get(ATTR_SALT).s();
          final String hash = item.get(ATTR_HASH).s();
          return Optional.of(new SaltedTokenHash(hash, salt));
        });
  }

  public CompletableFuture<Void> addOrReplace(final String number, final SaltedTokenHash data) {
    return asyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_E164, AttributeValues.fromString(number),
                ATTR_EXP, AttributeValues.fromLong(expirationSeconds()),
                ATTR_SALT, AttributeValues.fromString(data.salt()),
                ATTR_HASH, AttributeValues.fromString(data.hash())))
            .build())
        .thenRun(Util.NOOP);
  }

  public CompletableFuture<Void> removeEntry(final String number) {
    return asyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_E164, AttributeValues.fromString(number)))
            .build())
        .thenRun(Util.NOOP);
  }

  private long expirationSeconds() {
    return clock.instant().plus(expiration).getEpochSecond();
  }
}
