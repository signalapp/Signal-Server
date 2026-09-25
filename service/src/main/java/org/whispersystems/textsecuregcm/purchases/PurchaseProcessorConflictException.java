/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseProcessorConflictException extends PurchaseException {

  public PurchaseProcessorConflictException() {
    super(null, null);
  }

  public PurchaseProcessorConflictException(final String message) {
    super(null, message);
  }
}
