/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.ClientErrorException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

class IssuedReceiptsManagerTest {

  private static final long NOW_EPOCH_SECONDS = 1_500_000_000L;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.ISSUED_RECEIPTS);

  IssuedReceiptsManager issuedReceiptsManager;

  @BeforeEach
  void beforeEach() {
    issuedReceiptsManager = new IssuedReceiptsManager(
        Tables.ISSUED_RECEIPTS.tableName(),
        Duration.ofDays(90),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        TestRandomUtil.nextBytes(16));
  }

  @Test
  void testRecordIssuance() {
    Instant now = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);
    final ReceiptCredentialRequest receiptCredentialRequest = randomReceiptCredentialRequest();
    CompletableFuture<Void> future = issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE,
        receiptCredentialRequest, now);
    assertThat(future).succeedsWithin(Duration.ofSeconds(3));

    final Map<String, AttributeValue> item = getItem("item-1").item();
    final Set<byte[]> tagSet = item.get(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG_SET).bs()
        .stream()
        .map(SdkBytes::asByteArray)
        .collect(Collectors.toSet());
    assertThat(tagSet).containsExactly(item.get(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG).b().asByteArray());

    // same request should succeed
    future = issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE, receiptCredentialRequest,
        now);
    assertThat(future).succeedsWithin(Duration.ofSeconds(3));

    // same item with new request should fail
    byte[] request2 = TestRandomUtil.nextBytes(20);
    when(receiptCredentialRequest.serialize()).thenReturn(request2);
    future = issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE, receiptCredentialRequest,
        now);
    assertThat(future).failsWithin(Duration.ofSeconds(3)).
        withThrowableOfType(Throwable.class).
        havingCause().
        isExactlyInstanceOf(ClientErrorException.class).
        has(new Condition<>(
            e -> e instanceof ClientErrorException && ((ClientErrorException) e).getResponse().getStatus() == 409,
            "status 409"));

    // different item with new request should be okay though
    future = issuedReceiptsManager.recordIssuance("item-2", PaymentProvider.STRIPE, receiptCredentialRequest,
        now);
    assertThat(future).succeedsWithin(Duration.ofSeconds(3));
  }

  @Test
  void testMigrateToTagSet() {
    Instant now = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);

    issuedReceiptsManager
        .recordIssuance("itemId", PaymentProvider.STRIPE, randomReceiptCredentialRequest(), now)
        .join();
    removeTagSet("itemId");

    assertThat(getItem("itemId").item()).doesNotContainKey(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG_SET);

    final IssuedReceiptsManager.IssuedReceipt issuedReceipt = issuedReceiptsManager
        .receiptsWithoutTagSet(1, Schedulers.immediate())
        .blockFirst();

    issuedReceiptsManager.migrateToTagSet(issuedReceipt).join();

    final Map<String, AttributeValue> item = getItem("itemId").item();
    assertThat(item)
        .containsKey(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG_SET)
        .containsKey(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG);

    final List<byte[]> tags = item
        .get(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG_SET).bs()
        .stream()
        .map(SdkBytes::asByteArray)
        .toList();
    assertThat(tags).hasSize(1);

    final byte[] tag = item.get(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG).b().asByteArray();
    assertThat(tags).first().isEqualTo(tag);
  }


  @Test
  void testReceiptsWithoutTagSet() {
    Instant now = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);

    final int numItems = 100;
    final List<String> expectedNoTagSet = IntStream.range(0, numItems)
        .boxed()
        .flatMap(i -> {
          final String itemId = "item-%s".formatted(i);
          issuedReceiptsManager.recordIssuance(itemId, PaymentProvider.STRIPE, randomReceiptCredentialRequest(), now).join();

          if (i % 2 == 0) {
            removeTagSet(itemId);
            return Stream.of(itemId);
          } else {
            return Stream.empty();
          }
        }).toList();
    final List<String> items = issuedReceiptsManager
        .receiptsWithoutTagSet(1, Schedulers.immediate())
        .map(IssuedReceiptsManager.IssuedReceipt::itemId)
        .collectList().block();
    assertThat(items).hasSize(numItems / 2);
    assertThat(items).containsExactlyInAnyOrderElementsOf(expectedNoTagSet);
  }

  @Test
  void testMigrateAfterRecordExpires() {
    final IssuedReceiptsManager.IssuedReceipt issued = new IssuedReceiptsManager.IssuedReceipt("itemId",
        TestRandomUtil.nextBytes(32));
    // We should succeed but do nothing if the item is deleted by the time we try to migrate it
    issuedReceiptsManager.migrateToTagSet(issued).join();
    assertThat(getItem("itemId").hasItem()).isFalse();
  }

  private GetItemResponse getItem(final String itemId) {
    final DynamoDbClient client = DYNAMO_DB_EXTENSION.getDynamoDbClient();
    return client.getItem(GetItemRequest.builder()
        .tableName(Tables.ISSUED_RECEIPTS.tableName())
        .key(Map.of(IssuedReceiptsManager.KEY_PROCESSOR_ITEM_ID, AttributeValues.s(itemId)))
        .build());
  }

  private static ReceiptCredentialRequest randomReceiptCredentialRequest() {
    final ReceiptCredentialRequest request = mock(ReceiptCredentialRequest.class);
    final byte[] bytes = TestRandomUtil.nextBytes(20);
    when(request.serialize()).thenReturn(bytes);
    return request;
  }

  private void removeTagSet(final String itemId) {
    final DynamoDbClient client = DYNAMO_DB_EXTENSION.getDynamoDbClient();
    // Simulate an entry that was written before we wrote the tag set field
    client.updateItem(UpdateItemRequest.builder()
        .tableName(Tables.ISSUED_RECEIPTS.tableName())
        .key(Map.of(IssuedReceiptsManager.KEY_PROCESSOR_ITEM_ID, AttributeValues.s(itemId)))
        .updateExpression("REMOVE #tags")
        .expressionAttributeNames(Map.of("#tags", IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG_SET))
        .build());
  }
}
