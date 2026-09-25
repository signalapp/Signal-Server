/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseInvalidIdempotencyKeyException extends PurchaseInvalidArgumentsException {

  public PurchaseInvalidIdempotencyKeyException(final String message) {
    super(message);
  }
}
