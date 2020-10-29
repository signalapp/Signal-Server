/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

public class NotPushRegisteredException extends Exception {
  public NotPushRegisteredException(String s) {
    super(s);
  }

  public NotPushRegisteredException(Exception e) {
    super(e);
  }
}
