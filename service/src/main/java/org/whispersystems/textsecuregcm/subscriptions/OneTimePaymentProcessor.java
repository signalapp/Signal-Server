/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import java.io.IOException;
import java.util.Optional;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;

public interface OneTimePaymentProcessor {

  /// Retrieve information about a one-time purchase.
  ///
  /// @param paymentIdentifier A string that identifies the payment in the payment processor
  /// @return Details about the purchase, or empty if there was no corresponding payment
  Optional<PaymentDetails> claimOneTimePurchase(final String paymentIdentifier) throws IOException, RateLimitExceededException, SubscriptionInvalidArgumentsException;
}
