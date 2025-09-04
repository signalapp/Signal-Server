/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

/**
 * Attempted to retrieve a receipt for a subscription that hasn't yet been charged or the invoice is in the open state
 */
public class SubscriptionReceiptRequestedForOpenPaymentException extends SubscriptionException {

  public SubscriptionReceiptRequestedForOpenPaymentException() {
    super(null, null);
  }
}
