/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.whispersystems.textsecuregcm.storage.Account;

public record UserCapabilities(
    // TODO: Remove the paymentActivation capability entirely sometime soon after 2024-10-07
    boolean paymentActivation,
    boolean deleteSync,
    boolean versionedExpirationTimer) {

  public static UserCapabilities createForAccount(final Account account) {
    return new UserCapabilities(true, account.isDeleteSyncSupported(),
        account.isVersionedExpirationTimerSupported());
  }
}
