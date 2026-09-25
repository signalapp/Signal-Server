/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

import java.util.Optional;
import javax.annotation.Nullable;

public class PurchasePaymentRequiredException extends PurchaseException {

  private final PaymentProvider processor;
  @Nullable
  private final ChargeFailure chargeFailure;

  public PurchasePaymentRequiredException(final PaymentProvider processor) {
    this(processor, null, null);
  }

  public PurchasePaymentRequiredException(final PaymentProvider processor, final String message) {
    this(processor, null, message);
  }

  public PurchasePaymentRequiredException(final PaymentProvider processor, final ChargeFailure chargeFailure) {
    this(processor, chargeFailure, null);
  }

  private PurchasePaymentRequiredException(final PaymentProvider processor,
      @Nullable final ChargeFailure chargeFailure, @Nullable final String message) {
    super(null, message);
    this.processor = processor;
    this.chargeFailure = chargeFailure;
  }

  public PaymentProvider getProcessor() {
    return processor;
  }

  /// @return The reason the processor reported for the payment failure, if it reported one
  public Optional<ChargeFailure> getChargeFailure() {
    return Optional.ofNullable(chargeFailure);
  }
}
