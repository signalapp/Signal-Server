/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriptionInvalidAmountException extends SubscriptionInvalidArgumentsException {

  private String errorCode;

  public SubscriptionInvalidAmountException(String errorCode) {
    super(null, null);
    this.errorCode = errorCode;
  }

  public String getErrorCode() {
    return errorCode;
  }
}
