/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

class RedeemedReceiptsManagerTest {

  private static final long NOW_EPOCH_SECONDS = 1_500_000_000L;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.REDEEMED_RECEIPTS);

  Clock clock = TestClock.pinned(Instant.ofEpochSecond(NOW_EPOCH_SECONDS));
  ReceiptSerial receiptSerial;
  RedeemedReceiptsManager redeemedReceiptsManager;

  @BeforeEach
  void beforeEach() throws InvalidInputException {
    receiptSerial = new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE));
    redeemedReceiptsManager = new RedeemedReceiptsManager(
        clock,
        Tables.REDEEMED_RECEIPTS.tableName(),
        DYNAMO_DB_EXTENSION.getDynamoDbClient());
  }

  @Test
  void testPut() {
    final Instant receiptExpiration = Instant.ofEpochSecond(42);
    final long receiptLevel = 3;
    final UUID uuid1 = UUID.randomUUID();
    boolean put;

    // initial insert should return true
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel, uuid1);
    assertThat(put).isTrue();

    // subsequent attempted inserts with modified parameters should return false
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration.plusSeconds(1), receiptLevel, uuid1);
    assertThat(put).isFalse();
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel + 1, uuid1);
    assertThat(put).isFalse();

    final UUID uuid2 = UUID.randomUUID();
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel, uuid2);
    assertThat(put).isFalse();

    // repeated insert attempt of the original parameters should return true
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel, uuid1);
    assertThat(put).isTrue();

    // verify that the TTL is receipt expiration + padding
    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
        .tableName(Tables.REDEEMED_RECEIPTS.tableName())
        .key(Map.of(RedeemedReceiptsManager.KEY_SERIAL, AttributeValues.b(receiptSerial.serialize())))
        .build());
    assertThat(Long.parseLong(response.item().get(RedeemedReceiptsManager.ATTR_TTL).n())).isEqualTo(
        receiptExpiration.plus(RedeemedReceiptsManager.TTL_PADDING).getEpochSecond());
  }

  @Test
  void testBuildTransactWriteItemForReceipt() {
    final Instant receiptExpiration = Instant.ofEpochSecond(42);
    final long receiptLevel1 = 3;
    final UUID uuid1 = UUID.randomUUID();

    final TransactWriteItem writeItem = redeemedReceiptsManager.buildTransactWriteItemForReceipt(receiptSerial,
        receiptExpiration, receiptLevel1, uuid1);

    assertThatCode(() -> DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(writeItem)
        .build())).doesNotThrowAnyException();

    // A subsequent write with the same parameters should be idempotent
    assertThatCode(() -> DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(writeItem)
        .build())).doesNotThrowAnyException();

    // An attempt to insert the same receipt with a different UUID should fail
    final UUID uuid2 = UUID.randomUUID();
    final TransactWriteItem writeItem2 = redeemedReceiptsManager.buildTransactWriteItemForReceipt(receiptSerial,
        receiptExpiration, receiptLevel1, uuid2);
    assertThatThrownBy(
        () -> DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(writeItem2)
            .build()))
        .isInstanceOfSatisfying(TransactionCanceledException.class, e -> {
          final List<CancellationReason> cancellationReasons = e.cancellationReasons();
          assertThat(cancellationReasons).hasSize(1);
          assertThat(cancellationReasons.getFirst().code()).isEqualTo("ConditionalCheckFailed");
        });

    // An attempt to insert the same receipt with the original UUID but a different receipt level should fail
    final long receiptLevel2 = 4;
    final TransactWriteItem writeItem3 = redeemedReceiptsManager.buildTransactWriteItemForReceipt(receiptSerial,
        receiptExpiration, receiptLevel2, uuid1);
    assertThatThrownBy(
        () -> DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(writeItem3)
            .build()))
        .isInstanceOfSatisfying(TransactionCanceledException.class, e -> {
          final List<CancellationReason> cancellationReasons = e.cancellationReasons();
          assertThat(cancellationReasons).hasSize(1);
          assertThat(cancellationReasons.getFirst().code()).isEqualTo("ConditionalCheckFailed");
        });

    // verify that the TTL is receipt expiration + padding
    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
        .tableName(Tables.REDEEMED_RECEIPTS.tableName())
        .key(Map.of(RedeemedReceiptsManager.KEY_SERIAL, AttributeValues.b(receiptSerial.serialize())))
        .build());
    assertThat(Long.parseLong(response.item().get(RedeemedReceiptsManager.ATTR_TTL).n())).isEqualTo(
        receiptExpiration.plus(RedeemedReceiptsManager.TTL_PADDING).getEpochSecond());
  }
}
