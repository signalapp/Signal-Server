/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

/// An account was not found when it should have been (e.g. a previous lookup or authentication succeeded)
public class AccountNotFoundException extends RuntimeException {

  public AccountNotFoundException() {
    super();
  }
}
