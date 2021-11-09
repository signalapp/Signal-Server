/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class RedeemedReceiptsManager {

  public static final String KEY_SERIAL = "S";
  public static final String KEY_TTL = "E";
  public static final String KEY_RECEIPT_EXPIRATION = "G";
  public static final String KEY_RECEIPT_LEVEL = "L";
  public static final String KEY_ACCOUNT_UUID = "U";
  public static final String KEY_REDEMPTION_TIME = "R";

  private final Clock clock;
  private final String table;
  private final DynamoDbAsyncClient client;
  private final Duration expirationTime;

  public RedeemedReceiptsManager(
      @Nonnull final Clock clock,
      @Nonnull final String table,
      @Nonnull final DynamoDbAsyncClient client,
      @Nonnull final Duration expirationTime) {
    this.clock = Objects.requireNonNull(clock);
    this.table = Objects.requireNonNull(table);
    this.client = Objects.requireNonNull(client);
    this.expirationTime = Objects.requireNonNull(expirationTime);
  }

  /**
   * Returns true either if it's able to insert a new redeemed receipt entry with the {@code receiptExpiration}, {@code
   * receiptLevel}, and {@code accountUuid} provided or if an existing entry already exists with the same values thereby
   * allowing idempotent request processing.
   */
  public CompletableFuture<Boolean> put(
      @Nonnull final ReceiptSerial receiptSerial,
      final long receiptExpiration,
      final long receiptLevel,
      @Nonnull final UUID accountUuid) {

    // fail early if given bad inputs
    Objects.requireNonNull(receiptSerial);
    Objects.requireNonNull(accountUuid);

    final Instant now = clock.instant();
    final Instant rowExpiration = now.plus(expirationTime);
    final AttributeValue serialAttributeValue = AttributeValues.b(receiptSerial.serialize());

    final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_SERIAL, serialAttributeValue))
        .returnValues(ReturnValue.ALL_NEW)
        .updateExpression("SET #ttl = if_not_exists(#ttl, :ttl), "
            + "#receipt_expiration = if_not_exists(#receipt_expiration, :receipt_expiration), "
            + "#receipt_level = if_not_exists(#receipt_level, :receipt_level), "
            + "#account_uuid = if_not_exists(#account_uuid, :account_uuid), "
            + "#redemption_time = if_not_exists(#redemption_time, :redemption_time)")
        .expressionAttributeNames(Map.of(
            "#ttl", KEY_TTL,
            "#receipt_expiration", KEY_RECEIPT_EXPIRATION,
            "#receipt_level", KEY_RECEIPT_LEVEL,
            "#account_uuid", KEY_ACCOUNT_UUID,
            "#redemption_time", KEY_REDEMPTION_TIME))
        .expressionAttributeValues(Map.of(
            ":ttl", AttributeValues.n(rowExpiration.getEpochSecond()),
            ":receipt_expiration", AttributeValues.n(receiptExpiration),
            ":receipt_level", AttributeValues.n(receiptLevel),
            ":account_uuid", AttributeValues.b(accountUuid),
            ":redemption_time", AttributeValues.n(now.getEpochSecond())))
        .build();
    return client.updateItem(updateItemRequest).thenApply(updateItemResponse -> {
      final Map<String, AttributeValue> attributes = updateItemResponse.attributes();
      final long ddbReceiptExpiration = Long.parseLong(attributes.get(KEY_RECEIPT_EXPIRATION).n());
      final long ddbReceiptLevel = Long.parseLong(attributes.get(KEY_RECEIPT_LEVEL).n());
      final UUID ddbAccountUuid = UUIDUtil.fromByteBuffer(attributes.get(KEY_ACCOUNT_UUID).b().asByteBuffer());
      return ddbReceiptExpiration == receiptExpiration && ddbReceiptLevel == receiptLevel &&
          Objects.equals(ddbAccountUuid, accountUuid);
    });
  }
}
