/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseReceiptAlreadyRedeemedException extends PurchaseException {

  public PurchaseReceiptAlreadyRedeemedException() {
    super(null, null);
  }

  public PurchaseReceiptAlreadyRedeemedException(final String message) {
    super(null, message);
  }
}
