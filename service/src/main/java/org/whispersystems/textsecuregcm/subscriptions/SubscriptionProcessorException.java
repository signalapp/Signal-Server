/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionProcessorException extends Exception {

  private final SubscriptionProcessor processor;
  private final ChargeFailure chargeFailure;

  public SubscriptionProcessorException(final SubscriptionProcessor processor,
      final ChargeFailure chargeFailure) {
    this.processor = processor;
    this.chargeFailure = chargeFailure;
  }

  public SubscriptionProcessor getProcessor() {
    return processor;
  }

  public ChargeFailure getChargeFailure() {
    return chargeFailure;
  }
}
