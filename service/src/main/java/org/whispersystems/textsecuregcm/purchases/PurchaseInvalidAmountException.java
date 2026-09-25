/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseInvalidAmountException extends PurchaseInvalidArgumentsException {

  private String errorCode;

  public PurchaseInvalidAmountException(String errorCode) {
    super(null, null);
    this.errorCode = errorCode;
  }

  public String getErrorCode() {
    return errorCode;
  }
}
