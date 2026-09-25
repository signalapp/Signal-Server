/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseNotFoundException extends PurchaseException {

  public PurchaseNotFoundException() {
    super(null);
  }

  public PurchaseNotFoundException(Exception cause) {
    super(cause);
  }
}
