/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;
import static org.whispersystems.textsecuregcm.util.AttributeValues.s;

import com.google.common.base.Throwables;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.core.Response.Status;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class IssuedReceiptsManager {
  private static final Logger log = LoggerFactory.getLogger(IssuedReceiptsManager.class);

  public static final String KEY_PROCESSOR_ITEM_ID = "A";  // S  (HashKey)
  public static final String KEY_ISSUED_RECEIPT_TAG = "B";  // B
  public static final String KEY_EXPIRATION = "E";  // N

  public static final String KEY_ISSUED_RECEIPT_TAG_SET = "T"; // BS

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
   * For {@link PaymentProvider#STRIPE}, item is expected to refer to an invoice line item (subscriptions) or a
   * payment intent (one-time).
   */
  public CompletableFuture<Void> recordIssuance(
      String processorItemId,
      PaymentProvider processor,
      ReceiptCredentialRequest request,
      Instant now) {

    final AttributeValue key;
    if (processor == PaymentProvider.STRIPE) {
      // As the first processor, Stripe’s IDs were not prefixed. Its item IDs have documented prefixes (`il_`, `pi_`)
      // that will not collide with `SubscriptionProcessor` names
      key = s(processorItemId);
    } else {
      key = s(processor.name() + "_" + processorItemId);
    }
    final byte[] tag = generateIssuedReceiptTag(request);
    UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_PROCESSOR_ITEM_ID, key))
        .conditionExpression("attribute_not_exists(#key) OR #tag = :tag")
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET "
            + "#tag = if_not_exists(#tag, :tag), "
            + "#exp = if_not_exists(#exp, :exp) "
            + "ADD #tags :singletonTag")
        .expressionAttributeNames(Map.of(
            "#key", KEY_PROCESSOR_ITEM_ID,
            "#tag", KEY_ISSUED_RECEIPT_TAG,
            "#tags", KEY_ISSUED_RECEIPT_TAG_SET,
            "#exp", KEY_EXPIRATION))
        .expressionAttributeValues(Map.of(
            ":tag", b(tag),
            ":singletonTag", AttributeValue.fromBs(List.of(SdkBytes.fromByteArray(tag))),
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

  public CompletableFuture<Void> migrateToTagSet(final IssuedReceipt issuedReceipt) {
    UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_PROCESSOR_ITEM_ID, s(issuedReceipt.itemId())))
        .conditionExpression("attribute_exists(#key) AND #tag = :tag")
        .returnValues(ReturnValue.NONE)
        .updateExpression("ADD #tags :singletonTag")
        .expressionAttributeNames(Map.of(
            "#key", KEY_PROCESSOR_ITEM_ID,
            "#tag", KEY_ISSUED_RECEIPT_TAG,
            "#tags", KEY_ISSUED_RECEIPT_TAG_SET))
        .expressionAttributeValues(Map.of(
            ":tag", b(issuedReceipt.tag()),
            ":singletonTag", AttributeValue.fromBs(Collections.singletonList(SdkBytes.fromByteArray(issuedReceipt.tag())))))
        .build();
    return dynamoDbAsyncClient.updateItem(updateItemRequest)
        .thenRun(Util.NOOP)
        .exceptionally(ExceptionUtils.exceptionallyHandler(ConditionalCheckFailedException.class, e -> {
          log.info("Not migrating item {}, because when we tried to migrate it was already deleted", issuedReceipt.itemId());
          return null;
        }));
  }

  public record IssuedReceipt(String itemId, byte[] tag) {}
  public Flux<IssuedReceipt> receiptsWithoutTagSet(final int segments, final Scheduler scheduler) {
    if (segments < 1) {
      throw new IllegalArgumentException("Total number of segments must be positive");
    }

    return Flux.range(0, segments)
        .parallel()
        .runOn(scheduler)
        .flatMap(segment -> dynamoDbAsyncClient.scanPaginator(ScanRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .segment(segment)
                .totalSegments(segments)
                .filterExpression("attribute_not_exists(#tags)")
                .expressionAttributeNames(Map.of("#tags", KEY_ISSUED_RECEIPT_TAG_SET))
                .build())
            .items()
            .flatMapIterable(item -> {
              if (!item.containsKey(KEY_ISSUED_RECEIPT_TAG)) {
                log.error("Skipping item {} that was missing a receipt tag", item.get(KEY_PROCESSOR_ITEM_ID).s());
                return Collections.emptySet();
              }
              return List.of(new IssuedReceipt(item.get(KEY_PROCESSOR_ITEM_ID).s(), item.get(KEY_ISSUED_RECEIPT_TAG).b().asByteArray()));
            }))
        .sequential();
  }
}
