/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

public class ContactDiscoveryWriter extends AccountDatabaseCrawlerListener {

  private final AccountsManager accounts;

  public ContactDiscoveryWriter(final AccountsManager accounts) {
    this.accounts = accounts;
  }

  @Override
  public void onCrawlStart() {
    // nothing
  }

  @Override
  public void onCrawlEnd(final Optional<UUID> fromUuid) {
    // nothing
  }

  // We "update" by doing nothing, since everything about the account is already accurate except for a temporal
  // change in the 'shouldBeVisible' trait.  This update forces a new write of the underlying DB to reflect
  // that temporal change persistently.
  @VisibleForTesting
  static final Consumer<Account> NOOP_UPDATER = a -> {};

  @Override
  protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> chunkAccounts)
      throws AccountDatabaseCrawlerRestartException {
    for (Account account : chunkAccounts) {
      if (account.isCanonicallyDiscoverable() != account.shouldBeVisibleInDirectory()) {
        // It’s less than ideal, but crawler listeners currently must not call update()
        // with the accounts from the chunk, because updates cause the account instance to become stale. Instead, they
        // must get a new copy, which they are free to update.
        accounts.getByAccountIdentifier(account.getUuid()).ifPresent(a -> accounts.update(a, NOOP_UPDATER));
      }
    }
  }
}
