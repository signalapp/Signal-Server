/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionInvalidLevelException extends SubscriptionInvalidArgumentsException {

  public SubscriptionInvalidLevelException() {
    super(null, null);
  }
}
