/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
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
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.TestClock;

class RedeemedReceiptsManagerTest {

  private static final long NOW_EPOCH_SECONDS = 1_500_000_000L;
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.REDEEMED_RECEIPTS);

  Clock clock = TestClock.pinned(Instant.ofEpochSecond(NOW_EPOCH_SECONDS));
  ReceiptSerial receiptSerial;
  RedeemedReceiptsManager redeemedReceiptsManager;

  @BeforeEach
  void beforeEach() throws InvalidInputException {
    byte[] receiptSerialBytes = new byte[ReceiptSerial.SIZE];
    SECURE_RANDOM.nextBytes(receiptSerialBytes);
    receiptSerial = new ReceiptSerial(receiptSerialBytes);
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
