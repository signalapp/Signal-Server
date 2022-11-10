/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

public class AccountCleaner extends AccountDatabaseCrawlerListener {

  private static final Logger log = LoggerFactory.getLogger(AccountCleaner.class);

  private static final String DELETED_ACCOUNT_COUNTER_NAME = name(AccountCleaner.class, "deletedAccounts");
  private static final String DELETION_REASON_TAG_NAME = "reason";

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
    for (Account account : chunkAccounts) {
      if (isExpired(account) || needsExplicitRemoval(account)) {
        final String deletionReason = needsExplicitRemoval(account) ? "newlyExpired" : "previouslyExpired";

        try {
          accountsManager.delete(account, AccountsManager.DeletionReason.EXPIRED);
          Metrics.counter(DELETED_ACCOUNT_COUNTER_NAME, DELETION_REASON_TAG_NAME, deletionReason).increment();
        } catch (final Exception e) {
          log.warn("Failed to delete account {}", account.getUuid(), e);
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
