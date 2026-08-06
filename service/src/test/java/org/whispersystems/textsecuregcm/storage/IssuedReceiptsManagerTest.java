/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

class IssuedReceiptsManagerTest {

  private static final long EXPIRATION_EPOCH_SECONDS = 1_500_000_000L;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.ISSUED_RECEIPTS);

  private static final EnumMap<PaymentProvider, Integer> MAX_TAGS_MAP = new EnumMap<>(Map.of(
      PaymentProvider.STRIPE, 1,
      PaymentProvider.BRAINTREE, 2,
      PaymentProvider.GOOGLE_PLAY_BILLING, 3,
      PaymentProvider.APPLE_APP_STORE, 4));

  private IssuedReceiptsManager issuedReceiptsManager;

  @BeforeEach
  void beforeEach() {
    issuedReceiptsManager = new IssuedReceiptsManager(
        Tables.ISSUED_RECEIPTS.tableName(),
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        TestRandomUtil.nextBytes(16),
        MAX_TAGS_MAP);
  }

  @Test
  void testRecordIssuance() throws WriteConflictException {
    final Instant expiration = Instant.ofEpochSecond(EXPIRATION_EPOCH_SECONDS);
    final ReceiptCredentialRequest receiptCredentialRequest = randomReceiptCredentialRequest();
    issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE,
        receiptCredentialRequest, expiration);

    final Map<String, AttributeValue> item = getItem(PaymentProvider.STRIPE, "item-1").item();
    final Set<byte[]> tagSet = item.get(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG_SET).bs()
        .stream()
        .map(SdkBytes::asByteArray)
        .collect(Collectors.toSet());
    assertThat(tagSet).containsExactly(issuedReceiptsManager.generateIssuedReceiptTag(receiptCredentialRequest));

    // same request should succeed
    issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE, receiptCredentialRequest,
        expiration);

    // same item with new request should fail
    final byte[] request2 = TestRandomUtil.nextBytes(20);
    when(receiptCredentialRequest.serialize()).thenReturn(request2);
    assertThatThrownBy(
        () -> issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE, receiptCredentialRequest,
            expiration))
        .isExactlyInstanceOf(WriteConflictException.class);

    // different item with new request should be okay though
    issuedReceiptsManager.recordIssuance("item-2", PaymentProvider.STRIPE, receiptCredentialRequest,
        expiration);
  }

  @ParameterizedTest
  @EnumSource(PaymentProvider.class)
  void testIssueMax(final PaymentProvider processor) throws WriteConflictException {
    final Instant expiration = Instant.ofEpochSecond(EXPIRATION_EPOCH_SECONDS);

    final int maxTags = MAX_TAGS_MAP.get(processor);
    final List<ReceiptCredentialRequest> requests = IntStream.range(0, maxTags)
        .mapToObj(i -> randomReceiptCredentialRequest())
        .toList();
    for (int i = 0; i < maxTags; i++) {
      // Should be allowed to insert up to maxTags
        issuedReceiptsManager.recordIssuance("item-1", processor, requests.get(i), expiration);
      for (int j = 0; j < i; j++) {
        // Also should be allowed to repeat any previous tag
        issuedReceiptsManager.recordIssuance("item-1", processor, requests.get(j), expiration);
      }
    }

    assertThat(getItem(processor, "item-1").item().get(IssuedReceiptsManager.KEY_ISSUED_RECEIPT_TAG_SET).bs()
        .stream()
        .map(SdkBytes::asByteArray)
        .collect(Collectors.toSet()))
        .containsExactlyInAnyOrder(requests.stream()
            .map(issuedReceiptsManager::generateIssuedReceiptTag)
            .toArray(byte[][]::new));

    // Should not be allowed to insert past maxTags
    assertThatThrownBy(() -> issuedReceiptsManager.recordIssuance("item-1", processor, randomReceiptCredentialRequest(), expiration))
        .isExactlyInstanceOf(WriteConflictException.class);
  }

  @Test
  void testExpiration() throws WriteConflictException {
    final Instant expiration = Instant.ofEpochSecond(EXPIRATION_EPOCH_SECONDS);
    final ReceiptCredentialRequest receiptCredentialRequest = randomReceiptCredentialRequest();
    issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE, receiptCredentialRequest, expiration);

    final Map<String, AttributeValue> item = getItem(PaymentProvider.STRIPE, "item-1").item();
    assertThat(Instant.ofEpochSecond(AttributeValues.getLong(item, IssuedReceiptsManager.KEY_EXPIRATION, 0L)))
        .isEqualTo(expiration.plus(IssuedReceiptsManager.TTL_PADDING));

    // Only the initial TTL sticks (should never change anyways, because receipt expirations should be deterministic)
    issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE, receiptCredentialRequest, expiration.plus(Duration.ofDays(1)));
    assertThat(Instant.ofEpochSecond(AttributeValues.getLong(item, IssuedReceiptsManager.KEY_EXPIRATION, 0L)))
        .isEqualTo(expiration.plus(IssuedReceiptsManager.TTL_PADDING));

    issuedReceiptsManager.recordIssuance("item-1", PaymentProvider.STRIPE, receiptCredentialRequest, expiration.minus(Duration.ofDays(1)));
    assertThat(Instant.ofEpochSecond(AttributeValues.getLong(item, IssuedReceiptsManager.KEY_EXPIRATION, 0L)))
        .isEqualTo(expiration.plus(IssuedReceiptsManager.TTL_PADDING));
  }

  @ParameterizedTest
  @EnumSource(PaymentProvider.class)
  void testSingleIssuance(final PaymentProvider processor) throws WriteConflictException {
    final Instant expiration = Instant.ofEpochSecond(EXPIRATION_EPOCH_SECONDS);
    final ReceiptCredentialRequest request = randomReceiptCredentialRequest();

    issuedReceiptsManager.recordOneTimeIssuance("item-1", processor, request, expiration);
    // Retrying with the same request should still succeed
    issuedReceiptsManager.recordOneTimeIssuance("item-1", processor, request, expiration);
    assertThatThrownBy(
        () -> issuedReceiptsManager.recordOneTimeIssuance("item-1", processor, randomReceiptCredentialRequest(), expiration))
        .isExactlyInstanceOf(WriteConflictException.class);
  }

  private GetItemResponse getItem(final PaymentProvider processor, final String itemId) {
    final DynamoDbClient client = DYNAMO_DB_EXTENSION.getDynamoDbClient();
    return client.getItem(GetItemRequest.builder()
        .tableName(Tables.ISSUED_RECEIPTS.tableName())
        .key(Map.of(IssuedReceiptsManager.KEY_PROCESSOR_ITEM_ID, IssuedReceiptsManager.dynamoDbKey(processor, itemId)))
        .build());
  }

  private static ReceiptCredentialRequest randomReceiptCredentialRequest() {
    final ReceiptCredentialRequest request = mock(ReceiptCredentialRequest.class);
    final byte[] bytes = TestRandomUtil.nextBytes(20);
    when(request.serialize()).thenReturn(bytes);
    return request;
  }
}
