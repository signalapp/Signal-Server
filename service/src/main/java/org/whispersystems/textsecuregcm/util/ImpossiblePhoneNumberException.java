/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

public class ImpossiblePhoneNumberException extends Exception {

  public ImpossiblePhoneNumberException() {
    super();
  }

  public ImpossiblePhoneNumberException(final Throwable cause) {
    super(cause);
  }
}
