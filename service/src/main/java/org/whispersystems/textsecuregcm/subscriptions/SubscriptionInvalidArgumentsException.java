/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionInvalidArgumentsException extends SubscriptionException {

  public SubscriptionInvalidArgumentsException(final String message, final Exception cause) {
    super(cause, message);
  }

  public SubscriptionInvalidArgumentsException(final String message) {
    this(message, null);
  }
}
