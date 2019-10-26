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
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class AccountCleaner extends AccountDatabaseCrawlerListener {

  private static final MetricRegistry metricRegistry       = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          expiredAccountsMeter = metricRegistry.meter(name(AccountCleaner.class, "expiredAccounts"));

  @VisibleForTesting
  public static final int MAX_ACCOUNT_UPDATES_PER_CHUNK = 40;

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
  public void onCrawlEnd(Optional<UUID> fromUuid) {
  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) {
    int accountUpdateCount = 0;
    for (Account account : chunkAccounts) {
      if (needsExplicitRemoval(account)) {
        expiredAccountsMeter.mark();

        if (accountUpdateCount < MAX_ACCOUNT_UPDATES_PER_CHUNK) {
          Device masterDevice = account.getMasterDevice().get();
          masterDevice.setFetchesMessages(false);
          masterDevice.setApnId(null);
          masterDevice.setVoipApnId(null);
          masterDevice.setGcmId(null);

          accountUpdateCount++;
          accountsManager.update(account);

          directoryQueue.deleteRegisteredUser(account.getUuid(), account.getNumber());
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
