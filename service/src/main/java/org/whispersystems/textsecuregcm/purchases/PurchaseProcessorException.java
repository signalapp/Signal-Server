/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseProcessorException extends PurchaseException {

  private final PaymentProvider processor;
  private final ChargeFailure chargeFailure;

  public PurchaseProcessorException(final PaymentProvider processor, final ChargeFailure chargeFailure) {
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
