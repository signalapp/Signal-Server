/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;
import static org.whispersystems.textsecuregcm.util.AttributeValues.s;

import com.google.common.base.Throwables;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response.Status;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessor;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class IssuedReceiptsManager {

  public static final String KEY_PROCESSOR_ITEM_ID = "A";  // S  (HashKey)
  public static final String KEY_ISSUED_RECEIPT_TAG = "B";  // B
  public static final String KEY_EXPIRATION = "E";  // N

  private final String table;
  private final Duration expiration;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final byte[] receiptTagGenerator;

  public IssuedReceiptsManager(
      @Nonnull String table,
      @Nonnull Duration expiration,
      @Nonnull DynamoDbAsyncClient dynamoDbAsyncClient,
      @Nonnull byte[] receiptTagGenerator) {
    this.table = Objects.requireNonNull(table);
    this.expiration = Objects.requireNonNull(expiration);
    this.dynamoDbAsyncClient = Objects.requireNonNull(dynamoDbAsyncClient);
    this.receiptTagGenerator = Objects.requireNonNull(receiptTagGenerator);
  }

  /**
   * Returns a future that completes normally if either this processor item was never issued a receipt credential
   * previously OR if it was issued a receipt credential previously for the exact same receipt credential request
   * enabling clients to retry in case they missed the original response.
   * <p>
   * If this item has already been used to issue another receipt, throws a 409 conflict web application exception.
   * <p>
   * For {@link SubscriptionProcessor#STRIPE}, item is expected to refer to an invoice line item (subscriptions) or a
   * payment intent (one-time).
   */
  public CompletableFuture<Void> recordIssuance(
      String processorItemId,
      SubscriptionProcessor processor,
      ReceiptCredentialRequest request,
      Instant now) {

    final AttributeValue key;
    if (processor == SubscriptionProcessor.STRIPE) {
      // As the first processor, Stripe’s IDs were not prefixed. Its item IDs have documented prefixes (`il_`, `pi_`)
      // that will not collide with `SubscriptionProcessor` names
      key = s(processorItemId);
    } else {
      key = s(processor.name() + "_" + processorItemId);
    }
    UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_PROCESSOR_ITEM_ID, key))
        .conditionExpression("attribute_not_exists(#key) OR #tag = :tag")
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET "
            + "#tag = if_not_exists(#tag, :tag), "
            + "#exp = if_not_exists(#exp, :exp)")
        .expressionAttributeNames(Map.of(
            "#key", KEY_PROCESSOR_ITEM_ID,
            "#tag", KEY_ISSUED_RECEIPT_TAG,
            "#exp", KEY_EXPIRATION))
        .expressionAttributeValues(Map.of(
            ":tag", b(generateIssuedReceiptTag(request)),
            ":exp", n(now.plus(expiration).getEpochSecond())))
        .build();
    return dynamoDbAsyncClient.updateItem(updateItemRequest).handle((updateItemResponse, throwable) -> {
      if (throwable != null) {
        Throwable rootCause = Throwables.getRootCause(throwable);
        if (rootCause instanceof ConditionalCheckFailedException) {
          throw new ClientErrorException(Status.CONFLICT, rootCause);
        }
        Throwables.throwIfUnchecked(throwable);
        throw new CompletionException(throwable);
      }
      return null;
    });
  }

  private byte[] generateIssuedReceiptTag(ReceiptCredentialRequest request) {
    return generateHmac("issuedReceiptTag", mac -> mac.update(request.serialize()));
  }

  private byte[] generateHmac(String type, Consumer<Mac> byteConsumer) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(receiptTagGenerator, "HmacSHA256"));
      mac.update(type.getBytes(StandardCharsets.UTF_8));
      byteConsumer.accept(mac);
      return mac.doFinal();
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }
}
