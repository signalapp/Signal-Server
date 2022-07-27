/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

public class MessagePersistenceException extends Exception {

  public MessagePersistenceException(String message) {
    super(message);
  }
}
