/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.whispersystems.textsecuregcm.storage.Account;

public record UserCapabilities(
    // TODO: Remove the paymentActivation capability entirely sometime soon after 2024-06-30
    boolean paymentActivation,
    boolean deleteSync) {

  public static UserCapabilities createForAccount(final Account account) {
    return new UserCapabilities(true, account.isDeleteSyncSupported());
  }
}
