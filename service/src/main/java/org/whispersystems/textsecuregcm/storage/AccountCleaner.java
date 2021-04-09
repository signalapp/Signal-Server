/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class AccountCleaner extends AccountDatabaseCrawlerListener {

  private static final Logger log = LoggerFactory.getLogger(AccountCleaner.class);

  private static final MetricRegistry metricRegistry            = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          expiredAccountsMeter      = metricRegistry.meter(name(AccountCleaner.class, "expiredAccounts"));
  private static final Histogram      deletableAccountHistogram = metricRegistry.histogram(name(AccountCleaner.class, "deletableAccountsPerChunk"));

  @VisibleForTesting
  public static final int MAX_ACCOUNT_UPDATES_PER_CHUNK = 40;

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
    int accountUpdateCount    = 0;
    int deletableAccountCount = 0;

    for (Account account : chunkAccounts) {
      if (isExpired(account)) {
        deletableAccountCount++;
      }

      if (needsExplicitRemoval(account)) {
        expiredAccountsMeter.mark();

        if (accountUpdateCount < MAX_ACCOUNT_UPDATES_PER_CHUNK) {
          try {
            accountsManager.delete(account, AccountsManager.DeletionReason.EXPIRED);
            accountUpdateCount++;
          } catch (final Exception e) {
            log.warn("Failed to delete account {}", account.getUuid(), e);
          }
        }
      }
    }

    deletableAccountHistogram.update(deletableAccountCount);
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
