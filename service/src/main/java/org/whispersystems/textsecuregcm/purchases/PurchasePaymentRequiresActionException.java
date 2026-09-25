/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchasePaymentRequiresActionException extends PurchaseInvalidArgumentsException {

  public PurchasePaymentRequiresActionException(String message) {
    super(message, null);
  }

  public PurchasePaymentRequiresActionException() {
    super(null, null);
  }
}
