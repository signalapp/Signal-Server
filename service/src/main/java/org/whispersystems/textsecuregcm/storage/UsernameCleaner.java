/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class UsernameCleaner extends AccountDatabaseCrawlerListener {
  private static final String DELETED_USERNAME_COUNTER = name(UsernameCleaner.class, "deletedUsernames");
  private static final Logger logger = LoggerFactory.getLogger(UsernameCleaner.class);

  private final AccountsManager accountsManager;

  public UsernameCleaner(AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public void onCrawlStart() {
  }

  @Override
  protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> chunkAccounts) {
    for (Account account : chunkAccounts) {
      if (account.getUsername().isPresent()) {
        logger.info("Deleting username present for account {}", account.getUuid());
        try {
          this.accountsManager.clearUsername(account);
          Metrics.counter(DELETED_USERNAME_COUNTER, Tags.of("outcome", "success")).increment();
        } catch (Exception e) {
          logger.warn("Failed to clear username on account {}", account.getUuid(), e);
          Metrics.counter(DELETED_USERNAME_COUNTER, Tags.of("outcome", "error")).increment();
        }
      }
    }
  }

  @Override
  public void onCrawlEnd(final Optional<UUID> fromUuid) {
    logger.info("Username cleaner crawl completed");
  }
}
