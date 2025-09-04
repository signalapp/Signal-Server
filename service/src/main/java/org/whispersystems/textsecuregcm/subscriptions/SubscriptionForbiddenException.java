/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionForbiddenException extends SubscriptionException {

  public SubscriptionForbiddenException(final String message) {
    super(null, message);
  }
}
