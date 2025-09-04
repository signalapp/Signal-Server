/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionProcessorException extends SubscriptionException {

  private final PaymentProvider processor;
  private final ChargeFailure chargeFailure;

  public SubscriptionProcessorException(final PaymentProvider processor, final ChargeFailure chargeFailure) {
    super(null, null);
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
