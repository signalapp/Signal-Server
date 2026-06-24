/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.SubscriberIdCreationNotPermittedException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionForbiddenException;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionPaymentProcessor;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class SubscriptionManagerTest {

  private static final Subscriptions SUBSCRIPTIONS = mock(Subscriptions.class);
  private SubscriptionManager subscriptionManager;

  @BeforeEach
  void setUp() {
    reset(SUBSCRIPTIONS);

    final SubscriptionPaymentProcessor testProcessor = mock(SubscriptionPaymentProcessor.class);
    when(testProcessor.getProvider()).thenReturn(PaymentProvider.STRIPE);

    subscriptionManager = new SubscriptionManager(SUBSCRIPTIONS, List.of(testProcessor), mock(ServerZkReceiptOperations.class), mock(
        IssuedReceiptsManager.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void updateSubscriberCreate(final boolean createPermitted) {
    final byte[] user = TestRandomUtil.nextBytes(16);
    final SubscriberCredentials credentials = new SubscriberCredentials(TestRandomUtil.nextBytes(32),
        user,
        TestRandomUtil.nextBytes(16),
        TestRandomUtil.nextBytes(16),
        Instant.now());

    when(SUBSCRIPTIONS.get(any(byte[].class), any(byte[].class))).thenReturn(Subscriptions.GetResult.NOT_STORED);
    when(SUBSCRIPTIONS.create(any(byte[].class), any(byte[].class), any(Instant.class)))
        .thenReturn(mock(Subscriptions.Record.class));

    if (createPermitted) {
      assertDoesNotThrow(() -> subscriptionManager.updateSubscriber(credentials, createPermitted));
      verify(SUBSCRIPTIONS).create(aryEq(user), any(byte[].class), any(Instant.class));
    } else {
      assertThrows(SubscriberIdCreationNotPermittedException.class,
          () -> subscriptionManager.updateSubscriber(credentials, createPermitted));
      verify(SUBSCRIPTIONS, never()).create(any(byte[].class), any(byte[].class), any(Instant.class));
    }

  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void updateSubscriberExists(final boolean createPermitted) {
    final byte[] user = TestRandomUtil.nextBytes(16);
    final SubscriberCredentials credentials = new SubscriberCredentials(TestRandomUtil.nextBytes(32),
        user,
        TestRandomUtil.nextBytes(16),
        TestRandomUtil.nextBytes(16),
        Instant.now());

    when(SUBSCRIPTIONS.get(any(byte[].class), any(byte[].class))).thenReturn(Subscriptions.GetResult.found(mock(
        Subscriptions.Record.class)));
    when(SUBSCRIPTIONS.create(any(byte[].class), any(byte[].class), any(Instant.class)))
        .thenReturn(mock(Subscriptions.Record.class));

    assertDoesNotThrow(() -> subscriptionManager.updateSubscriber(credentials, createPermitted));
      verify(SUBSCRIPTIONS, never()).create(any(byte[].class), any(byte[].class), any(Instant.class));
    verify(SUBSCRIPTIONS).accessedAt(aryEq(user), any(Instant.class));
  }

  @Test
  void updateSubscriberMismatch() {
    final byte[] user = TestRandomUtil.nextBytes(16);
    final SubscriberCredentials credentials = new SubscriberCredentials(TestRandomUtil.nextBytes(32),
        user,
        TestRandomUtil.nextBytes(16),
        TestRandomUtil.nextBytes(16),
        Instant.now());

    when(SUBSCRIPTIONS.get(any(byte[].class), any(byte[].class))).thenReturn(Subscriptions.GetResult.PASSWORD_MISMATCH);

    assertThrows(SubscriptionForbiddenException.class,
        () -> subscriptionManager.updateSubscriber(credentials, true));
    verify(SUBSCRIPTIONS, never()).create(any(byte[].class), any(byte[].class), any(Instant.class));
  }

}
