/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.ClientErrorException;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequest;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessor;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;

class IssuedReceiptsManagerTest {

  private static final long NOW_EPOCH_SECONDS = 1_500_000_000L;
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.ISSUED_RECEIPTS);

  ReceiptCredentialRequest receiptCredentialRequest;
  IssuedReceiptsManager issuedReceiptsManager;

  @BeforeEach
  void beforeEach() {
    receiptCredentialRequest = mock(ReceiptCredentialRequest.class);
    byte[] generator = new byte[16];
    SECURE_RANDOM.nextBytes(generator);
    issuedReceiptsManager = new IssuedReceiptsManager(
        Tables.ISSUED_RECEIPTS.tableName(),
        Duration.ofDays(90),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        generator);
  }

  @Test
  void testRecordIssuance() {
    Instant now = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);
    byte[] request1 = new byte[20];
    SECURE_RANDOM.nextBytes(request1);
    when(receiptCredentialRequest.serialize()).thenReturn(request1);
    CompletableFuture<Void> future = issuedReceiptsManager.recordIssuance("item-1", SubscriptionProcessor.STRIPE,
        receiptCredentialRequest, now);
    assertThat(future).succeedsWithin(Duration.ofSeconds(3));

    // same request should succeed
    future = issuedReceiptsManager.recordIssuance("item-1", SubscriptionProcessor.STRIPE, receiptCredentialRequest,
        now);
    assertThat(future).succeedsWithin(Duration.ofSeconds(3));

    // same item with new request should fail
    byte[] request2 = new byte[20];
    SECURE_RANDOM.nextBytes(request2);
    when(receiptCredentialRequest.serialize()).thenReturn(request2);
    future = issuedReceiptsManager.recordIssuance("item-1", SubscriptionProcessor.STRIPE, receiptCredentialRequest,
        now);
    assertThat(future).failsWithin(Duration.ofSeconds(3)).
        withThrowableOfType(Throwable.class).
        havingCause().
        isExactlyInstanceOf(ClientErrorException.class).
        has(new Condition<>(
            e -> e instanceof ClientErrorException && ((ClientErrorException) e).getResponse().getStatus() == 409,
            "status 409"));

    // different item with new request should be okay though
    future = issuedReceiptsManager.recordIssuance("item-2", SubscriptionProcessor.STRIPE, receiptCredentialRequest,
        now);
    assertThat(future).succeedsWithin(Duration.ofSeconds(3));
  }
}
