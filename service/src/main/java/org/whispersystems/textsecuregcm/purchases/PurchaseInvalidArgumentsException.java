/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

public class PurchaseInvalidArgumentsException extends PurchaseException {

  public PurchaseInvalidArgumentsException(final String message, final Exception cause) {
    super(cause, message);
  }

  public PurchaseInvalidArgumentsException(final String message) {
    this(message, null);
  }
}
