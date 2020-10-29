/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

public class TransientPushFailureException extends Exception {
  public TransientPushFailureException(String s) {
    super(s);
  }

  public TransientPushFailureException(Exception e) {
    super(e);
  }
}
