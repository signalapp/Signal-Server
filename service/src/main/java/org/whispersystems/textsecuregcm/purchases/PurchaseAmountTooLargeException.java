/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseAmountTooLargeException extends PurchaseInvalidArgumentsException {
  private final String errorCode;

  public PurchaseAmountTooLargeException(final String errorCode) {
    super("Purchase amount too large");
    this.errorCode = errorCode;
  }

  public String getErrorCode() {
    return errorCode;
  }
}
