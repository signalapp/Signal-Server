/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionInvalidIdempotencyKeyException extends SubscriptionInvalidArgumentsException {

  public SubscriptionInvalidIdempotencyKeyException(final String message) {
    super(message);
  }
}
