package org.whispersystems.textsecuregcm.storage;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ContactDiscoveryWriter extends AccountDatabaseCrawlerListener {

  private final AccountStore accounts;

  public ContactDiscoveryWriter(final AccountStore accounts) {
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

  @Override
  protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> chunkAccounts)
      throws AccountDatabaseCrawlerRestartException {
    for (Account account : chunkAccounts) {
      if (account.isCanonicallyDiscoverable() != account.shouldBeVisibleInDirectory()) {
        // It’s less than ideal, but crawler listeners currently must not call update()
        // with the accounts from the chunk, because updates cause the account instance to become stale. Instead, they
        // must get a new copy, which they are free to update.
        accounts.get(account.getUuid()).ifPresent(accounts::update);
      }
    }
  }
}
