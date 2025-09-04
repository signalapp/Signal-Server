/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionChargeFailurePaymentRequiredException extends SubscriptionPaymentRequiredException {

  private final PaymentProvider processor;
  private final ChargeFailure chargeFailure;

  public SubscriptionChargeFailurePaymentRequiredException(final PaymentProvider processor,
      final ChargeFailure chargeFailure) {
    super();
    this.processor = processor;
    this.chargeFailure = chargeFailure;
  }

  public PaymentProvider getProcessor() {
    return processor;
  }

  public ChargeFailure getChargeFailure() {
    return chargeFailure;
  }

}
