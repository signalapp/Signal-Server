/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class OneTimeDonationsManager {
  public static final String KEY_PAYMENT_ID = "P"; // S
  public static final String ATTR_PAID_AT = "A"; // N
  private static final String ONETIME_DONATION_NOT_FOUND_COUNTER_NAME = name(OneTimeDonationsManager.class, "onetimeDonationNotFound");
  private final String table;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;

  public OneTimeDonationsManager(
      @Nonnull String table,
      @Nonnull DynamoDbAsyncClient dynamoDbAsyncClient) {
    this.table = Objects.requireNonNull(table);
    this.dynamoDbAsyncClient = Objects.requireNonNull(dynamoDbAsyncClient);
  }

  public CompletableFuture<Instant> getPaidAt(final String paymentId, final Instant fallbackTimestamp) {
    final GetItemRequest getItemRequest = GetItemRequest.builder()
        .consistentRead(Boolean.TRUE)
        .tableName(table)
        .key(Map.of(KEY_PAYMENT_ID, AttributeValues.fromString(paymentId)))
        .projectionExpression(ATTR_PAID_AT)
        .build();

    return dynamoDbAsyncClient.getItem(getItemRequest).thenApply(getItemResponse -> {
      if (!getItemResponse.hasItem()) {
        Metrics.counter(ONETIME_DONATION_NOT_FOUND_COUNTER_NAME).increment();
        return fallbackTimestamp;
      }

      return Instant.ofEpochSecond(AttributeValues.getLong(getItemResponse.item(), ATTR_PAID_AT, fallbackTimestamp.getEpochSecond()));
    });
  }

  public CompletableFuture<String> putPaidAt(final String paymentId, final Instant paidAt) {
    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(table)
            .item(Map.of(
                KEY_PAYMENT_ID, AttributeValues.fromString(paymentId),
                ATTR_PAID_AT, AttributeValues.fromLong(paidAt.getEpochSecond())))
            .build())
        .thenApply(unused -> paymentId);
  }
}
