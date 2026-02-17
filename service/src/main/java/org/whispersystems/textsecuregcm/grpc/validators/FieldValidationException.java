/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.validators;

public class FieldValidationException extends Exception {
  public FieldValidationException(String message) {
    super(message);
  }
}
