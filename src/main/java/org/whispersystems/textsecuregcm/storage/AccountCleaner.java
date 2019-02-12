/*
 * Copyright (C) 2019 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.assertj.core.util.VisibleForTesting;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class AccountCleaner implements AccountDatabaseCrawlerListener {
  private static final MetricRegistry metricRegistry       = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          expiredAccountsMeter = metricRegistry.meter(name(AccountCleaner.class, "expiredAccounts"));

  private final AccountsManager accountsManager;
  private final DirectoryQueue  directoryQueue;

  public AccountCleaner(AccountsManager accountsManager, DirectoryQueue directoryQueue) {
    this.accountsManager = accountsManager;
    this.directoryQueue  = directoryQueue;
  }

  @Override
  public void onCrawlStart() {
  }

  @Override
  public void onCrawlChunk(Optional<String> fromNumber, List<Account> chunkAccounts) {
    long nowMs = System.currentTimeMillis();
    for (Account account : chunkAccounts) {
      if (account.getMasterDevice().isPresent() &&
          account.getMasterDevice().get().isActive() &&
          isAccountExpired(account, nowMs))
      {
        expiredAccountsMeter.mark();

        Device masterDevice = account.getMasterDevice().get();
        masterDevice.setFetchesMessages(false);
        masterDevice.setApnId(null);
        masterDevice.setGcmId(null);

        accountsManager.update(account);
        directoryQueue.deleteRegisteredUser(account.getNumber());
      }
    }
  }

  @Override
  public void onCrawlEnd(Optional<String> fromNumber) {
  }

  @VisibleForTesting
  public static boolean isAccountExpired(Account account, long nowMs) {
    return account.getLastSeen() < (nowMs - TimeUnit.DAYS.toMillis(365));
  }

}
