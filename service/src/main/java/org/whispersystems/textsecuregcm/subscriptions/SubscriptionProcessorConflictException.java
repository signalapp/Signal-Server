/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionProcessorConflictException extends SubscriptionException {

  public SubscriptionProcessorConflictException() {
    super(null, null);
  }

  public SubscriptionProcessorConflictException(final String message) {
    super(null, message);
  }
}
