/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

import java.io.IOException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;

public interface OneTimePaymentProcessor {

  /// Retrieve information about a one-time purchase.
  ///
  /// @param paymentIdentifier A string that identifies the payment in the payment processor
  /// @return Details about the purchase if it was successful
  ///
  /// @throws PurchaseInvalidArgumentsException The purchase does not match a configured product type
  /// @throws PurchaseNotFoundException The paymentIdentifier does not identify a transaction in the processor
  /// @throws PurchasePaymentRequiredException The identified payment failed or was revoked
  /// @throws PurchaseReceiptRequestedForOpenPaymentException The identified payment is still processing
  PaymentDetails claimOneTimePurchase(final String paymentIdentifier) throws IOException, RateLimitExceededException, PurchaseInvalidArgumentsException, PurchaseNotFoundException, PurchasePaymentRequiredException, PurchaseReceiptRequestedForOpenPaymentException;
}
