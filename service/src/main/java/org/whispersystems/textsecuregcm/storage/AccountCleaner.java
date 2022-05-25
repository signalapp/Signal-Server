/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class AccountCleaner extends AccountDatabaseCrawlerListener {

  private static final Logger log = LoggerFactory.getLogger(AccountCleaner.class);

  private static final String DELETED_ACCOUNT_COUNTER_NAME = name(AccountCleaner.class, "deletedAccounts");
  private static final String DELETION_REASON_TAG_NAME = "reason";

  @VisibleForTesting
  static final int MAX_ACCOUNT_DELETIONS_PER_CHUNK = 256;

  private final AccountsManager accountsManager;

  public AccountCleaner(AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public void onCrawlStart() {
  }

  @Override
  public void onCrawlEnd(Optional<UUID> fromUuid) {
  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) {
    int accountUpdateCount = 0;

    for (Account account : chunkAccounts) {
      if (isExpired(account) || needsExplicitRemoval(account)) {
        final Tag deletionReason;

        if (needsExplicitRemoval(account)) {
          deletionReason = Tag.of(DELETION_REASON_TAG_NAME, "newlyExpired");
        } else {
          deletionReason = Tag.of(DELETION_REASON_TAG_NAME, "previouslyExpired");
        }

        if (accountUpdateCount < MAX_ACCOUNT_DELETIONS_PER_CHUNK) {
          try {
            accountsManager.delete(account, AccountsManager.DeletionReason.EXPIRED);
            accountUpdateCount++;

            Metrics.counter(DELETED_ACCOUNT_COUNTER_NAME, Tags.of(deletionReason)).increment();
          } catch (final Exception e) {
            log.warn("Failed to delete account {}", account.getUuid(), e);
          }
        }
      }
    }
  }

  private boolean needsExplicitRemoval(Account account) {
    return account.getMasterDevice().isPresent()           &&
           hasPushToken(account.getMasterDevice().get())   &&
           isExpired(account);
  }

  private boolean hasPushToken(Device device) {
    return !Util.isEmpty(device.getGcmId()) || !Util.isEmpty(device.getApnId()) || !Util.isEmpty(device.getVoipApnId()) || device.getFetchesMessages();
  }

  private boolean isExpired(Account account) {
    return account.getLastSeen() + TimeUnit.DAYS.toMillis(365) < System.currentTimeMillis();
  }

}
