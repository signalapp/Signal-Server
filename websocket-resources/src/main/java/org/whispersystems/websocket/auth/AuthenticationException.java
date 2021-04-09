/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

public class AuthenticationException extends Exception {

  public AuthenticationException(String s) {
    super(s);
  }

  public AuthenticationException(Exception e) {
    super(e);
  }

}
