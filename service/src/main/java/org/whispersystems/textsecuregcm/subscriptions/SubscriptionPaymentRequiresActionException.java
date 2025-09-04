/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionPaymentRequiresActionException extends SubscriptionInvalidArgumentsException {

  public SubscriptionPaymentRequiresActionException(String message) {
    super(message, null);
  }

  public SubscriptionPaymentRequiresActionException() {
    super(null, null);
  }
}
