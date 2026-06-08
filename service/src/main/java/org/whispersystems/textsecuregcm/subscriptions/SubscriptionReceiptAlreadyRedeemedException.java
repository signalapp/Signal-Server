/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionReceiptAlreadyRedeemedException extends SubscriptionException {

  public SubscriptionReceiptAlreadyRedeemedException() {
    super(null, null);
  }

  public SubscriptionReceiptAlreadyRedeemedException(final String message) {
    super(null, message);
  }
}
