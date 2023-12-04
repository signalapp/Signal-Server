/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

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
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Duration.ofDays(90));
  }

  @Test
  void testPut() throws ExecutionException, InterruptedException {
    final long receiptExpiration = 42;
    final long receiptLevel = 3;
    CompletableFuture<Boolean> put;

    // initial insert should return true
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel, AuthHelper.VALID_UUID);
    assertThat(put.get()).isTrue();

    // subsequent attempted inserts with modified parameters should return false
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration + 1, receiptLevel, AuthHelper.VALID_UUID);
    assertThat(put.get()).isFalse();
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel + 1, AuthHelper.VALID_UUID);
    assertThat(put.get()).isFalse();
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel, AuthHelper.VALID_UUID_TWO);
    assertThat(put.get()).isFalse();

    // repeated insert attempt of the original parameters should return true
    put = redeemedReceiptsManager.put(receiptSerial, receiptExpiration, receiptLevel, AuthHelper.VALID_UUID);
    assertThat(put.get()).isTrue();
  }
}
