/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import java.util.UUID;

public class NoSuchUserException extends Exception {

  public NoSuchUserException(final UUID uuid) {
    super(uuid.toString());
  }

  public NoSuchUserException(Exception e) {
    super(e);
  }
}
