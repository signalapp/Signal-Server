/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

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

  public CompletableFuture<Void> addOrReplace(final UUID phoneNumberIdentifier, final SaltedTokenHash data) {
    final long expirationSeconds = expirationSeconds();

    return asyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_PNI, AttributeValues.fromString(phoneNumberIdentifier.toString()),
                ATTR_EXP, AttributeValues.fromLong(expirationSeconds),
                ATTR_SALT, AttributeValues.fromString(data.salt()),
                ATTR_HASH, AttributeValues.fromString(data.hash())))
            .build())
        .thenRun(Util.NOOP);
  }

  CompletableFuture<Void> removeEntry(final String number) {
    return asyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_PNI, AttributeValues.fromString(number)))
            .build())
        .thenRun(Util.NOOP);
  }

  public CompletableFuture<Void> removeEntry(final UUID phoneNumberIdentifier) {
    return asyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_PNI, AttributeValues.fromString(phoneNumberIdentifier.toString())))
            .build())
        .thenRun(Util.NOOP);
  }

  @VisibleForTesting
  long expirationSeconds() {
    return clock.instant().plus(expiration).getEpochSecond();
  }

  Flux<String> getE164sWithRegistrationRecoveryPasswords(final int segments, final int bufferSize, final Scheduler scheduler) {
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
                .expressionAttributeNames(Map.of("#key", KEY_PNI))
                .expressionAttributeValues(Map.of(":e164Prefix", AttributeValue.fromS("+")))
                .build())
            .items()
            .map(item -> item.get(KEY_PNI).s()))
        .sequential()
        .buffer(bufferSize)
        .map(source -> {
          final List<String> shuffled = new ArrayList<>(source);
          Collections.shuffle(shuffled);
          return shuffled;
        })
        .limitRate(2)
        .flatMapIterable(Function.identity());
  }

  private static SaltedTokenHash saltedTokenHashFromItem(final Map<String, AttributeValue> item) {
    return new SaltedTokenHash(item.get(ATTR_HASH).s(), item.get(ATTR_SALT).s());
  }
}
