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
import javax.annotation.Nonnull;
import com.google.common.annotations.VisibleForTesting;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class RedeemedReceiptsManager {

  public static final String KEY_SERIAL = "S";
  public static final String ATTR_TTL = "E";
  public static final String ATTR_RECEIPT_EXPIRATION = "G";
  public static final String ATTR_RECEIPT_LEVEL = "L";
  public static final String ATTR_ACCOUNT_UUID = "U";
  public static final String ATTR_REDEMPTION_TIME = "R";

  private final Clock clock;
  private final String table;
  private final DynamoDbClient client;

  @VisibleForTesting
  static final Duration TTL_PADDING = Duration.ofDays(30);

  public RedeemedReceiptsManager(
      @Nonnull final Clock clock,
      @Nonnull final String table,
      @Nonnull final DynamoDbClient client) {
    this.clock = Objects.requireNonNull(clock);
    this.table = Objects.requireNonNull(table);
    this.client = Objects.requireNonNull(client);
  }

  /**
   * Returns true either if it's able to insert a new redeemed receipt entry with the {@code receiptExpiration}, {@code
   * receiptLevel}, and {@code accountUuid} provided or if an existing entry already exists with the same values thereby
   * allowing idempotent request processing.
   */
  public boolean put(
      @Nonnull final ReceiptSerial receiptSerial,
      final Instant receiptExpiration,
      final long receiptLevel,
      @Nonnull final UUID accountUuid) {

    // fail early if given bad inputs
    Objects.requireNonNull(receiptSerial);
    Objects.requireNonNull(accountUuid);

    final Instant now = clock.instant();
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
            "#ttl", ATTR_TTL,
            "#receipt_expiration", ATTR_RECEIPT_EXPIRATION,
            "#receipt_level", ATTR_RECEIPT_LEVEL,
            "#account_uuid", ATTR_ACCOUNT_UUID,
            "#redemption_time", ATTR_REDEMPTION_TIME))
        .expressionAttributeValues(Map.of(
            ":ttl", rowTtl(receiptExpiration),
            ":receipt_expiration", AttributeValues.n(receiptExpiration.getEpochSecond()),
            ":receipt_level", AttributeValues.n(receiptLevel),
            ":account_uuid", AttributeValues.b(accountUuid),
            ":redemption_time", AttributeValues.n(now.getEpochSecond())))
        .build();
    final UpdateItemResponse updateItemResponse = client.updateItem(updateItemRequest);

      final Map<String, AttributeValue> attributes = updateItemResponse.attributes();
      final long ddbReceiptExpiration = Long.parseLong(attributes.get(ATTR_RECEIPT_EXPIRATION).n());
      final long ddbReceiptLevel = Long.parseLong(attributes.get(ATTR_RECEIPT_LEVEL).n());
      final UUID ddbAccountUuid = UUIDUtil.fromByteBuffer(attributes.get(ATTR_ACCOUNT_UUID).b().asByteBuffer());
      return ddbReceiptExpiration == receiptExpiration.getEpochSecond() && ddbReceiptLevel == receiptLevel &&
          Objects.equals(ddbAccountUuid, accountUuid);
  }

  /// Creates a [TransactWriteItem] that inserts the receipt into the redeemed receipts table if the receipt serial
  /// mapping for the specified accountUuid doesn't already exist. If a receipt serial mapping exists for another
  /// account, the enclosing transaction fails.
  ///
  /// @param receiptSerial     The receipt serial
  /// @param receiptExpiration The timestamp at which the receipt expires
  /// @param receiptLevel      The receipt level indicating the type of entitlement
  /// @param accountUuid       The account identifier
  public TransactWriteItem buildTransactWriteItemForReceipt(
      final ReceiptSerial receiptSerial,
      final Instant receiptExpiration,
      final long receiptLevel,
      final UUID accountUuid) {

    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(table)
            .item(Map.of(
                KEY_SERIAL, AttributeValues.b(receiptSerial.serialize()),
                ATTR_TTL, rowTtl(receiptExpiration),
                ATTR_RECEIPT_EXPIRATION, AttributeValues.n(receiptExpiration.getEpochSecond()),
                ATTR_RECEIPT_LEVEL, AttributeValues.n(receiptLevel),
                ATTR_ACCOUNT_UUID, AttributeValues.b(accountUuid),
                ATTR_REDEMPTION_TIME, AttributeValues.n(clock.instant().getEpochSecond())))
            .conditionExpression("attribute_not_exists(#serial) OR (#account_uuid = :account_uuid AND #receipt_level = :receipt_level)")
            .expressionAttributeNames(Map.of(
                "#serial", KEY_SERIAL,
                "#account_uuid", ATTR_ACCOUNT_UUID,
                "#receipt_level", ATTR_RECEIPT_LEVEL))
            .expressionAttributeValues(Map.of(
                ":account_uuid", AttributeValues.b(accountUuid),
                ":receipt_level", AttributeValues.n(receiptLevel)
            ))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build();
  }

  private static AttributeValue rowTtl(final Instant receiptExpiration) {
    return AttributeValues.n(receiptExpiration.plus(TTL_PADDING).getEpochSecond());
  }
}
