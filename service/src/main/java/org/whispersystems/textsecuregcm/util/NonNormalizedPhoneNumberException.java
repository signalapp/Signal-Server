/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

public class NonNormalizedPhoneNumberException extends Exception {

  private final String originalNumber;
  private final String normalizedNumber;

  public NonNormalizedPhoneNumberException(final String originalNumber, final String normalizedNumber) {
    this.originalNumber = originalNumber;
    this.normalizedNumber = normalizedNumber;
  }

  public String getOriginalNumber() {
    return originalNumber;
  }

  public String getNormalizedNumber() {
    return normalizedNumber;
  }
}
