/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.tests.util.SubscriptionConfigTestHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;

class AbstractV1SubscriptionControllerTest {

  static final Clock CLOCK = mock(Clock.class);

  static final OneTimeDonationConfiguration ONETIME_CONFIG = SubscriptionConfigTestHelper.getOneTimeConfig();
  static final StripeManager STRIPE_MANAGER = MockUtils.buildMock(StripeManager.class, mgr ->
      when(mgr.getProvider()).thenReturn(PaymentProvider.STRIPE));
  static final BraintreeManager BRAINTREE_MANAGER = MockUtils.buildMock(BraintreeManager.class, mgr ->
      when(mgr.getProvider()).thenReturn(PaymentProvider.BRAINTREE));
  static final IssuedReceiptsManager ISSUED_RECEIPTS_MANAGER = mock(IssuedReceiptsManager.class);

  static final ServerZkReceiptOperations ZK_OPS = mock(ServerZkReceiptOperations.class);

}

