/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

public class RefreshingAccountNotFoundException extends RuntimeException {

  public RefreshingAccountNotFoundException(final String message) {
    super(message);
  }

}
