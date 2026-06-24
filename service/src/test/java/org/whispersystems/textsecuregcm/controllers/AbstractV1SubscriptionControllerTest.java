/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequest;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequestContext;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.OneTimeDonationConfiguration;
import org.whispersystems.textsecuregcm.storage.DonationPermits;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
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

  private static final ServerSecretParams DONATION_PERMITS_SECRET_PARAMS = ServerSecretParams.generate();
  static final DonationPermits DONATION_PERMITS = mock(DonationPermits.class);
  static final DonationPermitsManager DONATION_PERMITS_MANAGER = new DonationPermitsManager(DONATION_PERMITS, DONATION_PERMITS_SECRET_PARAMS, CLOCK);

  String getDonationPermitHeader() {
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(1);
    final DonationPermitRequest permitRequest = context.request();

    final DonationPermitResponse permitResponse = DONATION_PERMITS_MANAGER.issue(permitRequest);

    try {
      final List<DonationPermit> donationPermits = context.receive(permitResponse,
          DONATION_PERMITS_SECRET_PARAMS.getPublicParams(), CLOCK.instant());

      return Base64.getEncoder().encodeToString(donationPermits.getFirst().serialize());
    } catch (final VerificationFailedException e) {
      throw new RuntimeException(e);
    }
  }

  /// Sets up stubbing so that spendIds are spend-once
  protected void setUpDonationPermitsSpendStubbing(final DonationPermits donationPermits) {
    final Set<String> spent = new HashSet<>();
    when(donationPermits.spend(any(byte[].class), any(Instant.class)))
        .thenAnswer(answer -> spent.add(new String(answer.getArgument(0, byte[].class))));
  }

}

