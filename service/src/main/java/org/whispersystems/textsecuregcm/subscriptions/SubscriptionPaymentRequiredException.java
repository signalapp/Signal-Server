/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionPaymentRequiredException extends SubscriptionException {

  public SubscriptionPaymentRequiredException() {
    super(null, null);
  }

  public SubscriptionPaymentRequiredException(String message) {
    super(null, message);
  }
}
