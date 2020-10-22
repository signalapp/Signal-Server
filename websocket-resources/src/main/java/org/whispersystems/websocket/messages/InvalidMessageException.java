/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.messages;

public class InvalidMessageException extends Exception {
  public InvalidMessageException(String s) {
    super(s);
  }

  public InvalidMessageException(Exception e) {
    super(e);
  }
}
