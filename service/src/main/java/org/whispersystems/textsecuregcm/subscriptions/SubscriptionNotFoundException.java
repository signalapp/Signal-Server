/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionNotFoundException extends SubscriptionException {

  public SubscriptionNotFoundException() {
    super(null);
  }

  public SubscriptionNotFoundException(Exception cause) {
    super(cause);
  }
}
