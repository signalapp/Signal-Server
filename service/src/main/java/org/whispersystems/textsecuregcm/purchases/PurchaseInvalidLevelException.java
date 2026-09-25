/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseInvalidLevelException extends PurchaseInvalidArgumentsException {

  public PurchaseInvalidLevelException() {
    super(null, null);
  }
}
