/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.whispersystems.textsecuregcm.storage.Account;

public record UserCapabilities(
    boolean deleteSync,
    boolean versionedExpirationTimer) {

  public static UserCapabilities createForAccount(final Account account) {
    return new UserCapabilities(account.isDeleteSyncSupported(),
        account.isVersionedExpirationTimerSupported());
  }
}
