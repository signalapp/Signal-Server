/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;


public class InvalidAuthorizationHeaderException extends Exception {
  public InvalidAuthorizationHeaderException(String s) {
    super(s);
  }

  public InvalidAuthorizationHeaderException(Exception e) {
    super(e);
  }
}
