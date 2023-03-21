/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securevaluerecovery;

public class SecureValueRecoveryException extends RuntimeException {

  public SecureValueRecoveryException(final String message) {
    super(message);
  }
}
