/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;
import static org.whispersystems.textsecuregcm.util.AttributeValues.s;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class IssuedReceiptsManager {

  public static final String KEY_PROCESSOR_ITEM_ID = "A";  // S  (HashKey)
  public static final String KEY_EXPIRATION = "E";  // N
  public static final String KEY_ISSUED_RECEIPT_TAG_SET = "T"; // BS

  private final String table;
  private final Duration expiration;
  private final DynamoDbClient dynamoDbClient;
  private final byte[] receiptTagGenerator;
  private final EnumMap<PaymentProvider, Integer> maxIssuedReceiptsPerPaymentId;

  public IssuedReceiptsManager(
      @Nonnull final String table,
      @Nonnull final Duration expiration,
      @Nonnull final DynamoDbClient dynamoDbClient,
      @Nonnull final byte[] receiptTagGenerator,
      @Nonnull final EnumMap<PaymentProvider, Integer> maxIssuedReceiptsPerPaymentId) {
    this.table = Objects.requireNonNull(table);
    this.expiration = Objects.requireNonNull(expiration);
    this.dynamoDbClient = Objects.requireNonNull(dynamoDbClient);
    this.receiptTagGenerator = Objects.requireNonNull(receiptTagGenerator);
    this.maxIssuedReceiptsPerPaymentId = Objects.requireNonNull(maxIssuedReceiptsPerPaymentId);
  }

  /// Returns normally if either this processor item was never issued a receipt credential
  /// previously OR if it was issued a receipt credential previously for the exact same receipt credential request
  /// enabling clients to retry in case they missed the original response.
  ///
  /// If this item has already been used to issue another receipt, throws [WriteConflictException].
  ///
  /// For [PaymentProvider#STRIPE], item is expected to refer to an invoice line item (subscriptions) or a
  /// payment intent (one-time).
  public void recordIssuance(
      final String processorItemId,
      final PaymentProvider processor,
      final ReceiptCredentialRequest request,
      final Instant now) throws WriteConflictException {

    final AttributeValue key = dynamoDbKey(processor, processorItemId);
    final byte[] tag = generateIssuedReceiptTag(request);
    final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_PROCESSOR_ITEM_ID, key))
        .conditionExpression("attribute_not_exists(#key) OR contains(#tags, :tag) OR size(#tags) < :maxTags")
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET #exp = if_not_exists(#exp, :exp) ADD #tags :singletonTag")
        .expressionAttributeNames(Map.of(
            "#key", KEY_PROCESSOR_ITEM_ID,
            "#tags", KEY_ISSUED_RECEIPT_TAG_SET,
            "#exp", KEY_EXPIRATION))
        .expressionAttributeValues(Map.of(
            ":tag", b(tag),
            ":singletonTag", AttributeValue.fromBs(List.of(SdkBytes.fromByteArray(tag))),
            ":exp", n(now.plus(expiration).getEpochSecond()),
            ":maxTags", n(maxIssuedReceiptsPerPaymentId.get(processor))))
        .build();
    try {
      dynamoDbClient.updateItem(updateItemRequest);
    } catch (final ConditionalCheckFailedException _) {
      throw new WriteConflictException();
    }
  }

  /// Clear the recorded issuances for a particular item
  ///
  /// @param processorItemId The itemId within the processor to clear
  /// @param processor The processor
  /// @return true if the item was deleted, false if the item did not exist
  public boolean clearIssuance(final String processorItemId, final PaymentProvider processor) {
    final AttributeValue key = dynamoDbKey(processor, processorItemId);
    final DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_PROCESSOR_ITEM_ID, key))
        .returnValues(ReturnValue.ALL_OLD)
        .build();
    final DeleteItemResponse item = dynamoDbClient.deleteItem(deleteItemRequest);
    return item.hasAttributes() && !item.attributes().isEmpty();
  }

  @VisibleForTesting
  static AttributeValue dynamoDbKey(final PaymentProvider processor, final String processorItemId) {
    if (processor == PaymentProvider.STRIPE) {
      // As the first processor, Stripe’s IDs were not prefixed. Its item IDs have documented prefixes (`il_`, `pi_`)
      // that will not collide with `SubscriptionProcessor` names
      return s(processorItemId);
    } else {
      return s(processor.name() + "_" + processorItemId);
    }
  }


  @VisibleForTesting
  byte[] generateIssuedReceiptTag(final ReceiptCredentialRequest request) {
    return generateHmac("issuedReceiptTag", mac -> mac.update(request.serialize()));
  }

  private byte[] generateHmac(final String type, final Consumer<Mac> byteConsumer) {
    try {
      final Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(receiptTagGenerator, "HmacSHA256"));
      mac.update(type.getBytes(StandardCharsets.UTF_8));
      byteConsumer.accept(mac);
      return mac.doFinal();
    } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }
}
