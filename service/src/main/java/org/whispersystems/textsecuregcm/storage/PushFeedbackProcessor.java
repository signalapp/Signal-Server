/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

public class PushFeedbackProcessor extends AccountDatabaseCrawlerListener {

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          expired        = metricRegistry.meter(name(getClass(), "unregistered", "expired"));
  private final Meter          recovered      = metricRegistry.meter(name(getClass(), "unregistered", "recovered"));

  private final AccountsManager accountsManager;
  private final DirectoryQueue  directoryQueue;

  public PushFeedbackProcessor(AccountsManager accountsManager, DirectoryQueue directoryQueue) {
    this.accountsManager = accountsManager;
    this.directoryQueue  = directoryQueue;
  }

  @Override
  public void onCrawlStart() {}

  @Override
  public void onCrawlEnd(Optional<UUID> toUuid) {}

  @Override
  protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) {
    final List<Account> directoryUpdateAccounts = new ArrayList<>();

    for (Account account : chunkAccounts) {
      boolean update = false;

      final Set<Device> devices = account.getDevices();
      for (Device device : devices) {
        if (deviceNeedsUpdate(device)) {
          if (deviceExpired(device)) {
            expired.mark();
          } else {
            recovered.mark();
          }
          update = true;
        }
      }

      if (update) {
        account = accountsManager.update(account, a -> {
          for (Device device: a.getDevices()) {
            if (deviceNeedsUpdate(device)) {
              if (deviceExpired(device)) {
                if (!Util.isEmpty(device.getApnId())) {
                  if (device.getId() == 1) {
                    device.setUserAgent("OWI");
                  } else {
                    device.setUserAgent("OWP");
                  }
                } else if (!Util.isEmpty(device.getGcmId())) {
                  device.setUserAgent("OWA");
                }
                device.setGcmId(null);
                device.setApnId(null);
                device.setVoipApnId(null);
                device.setFetchesMessages(false);
              } else {
                device.setUninstalledFeedbackTimestamp(0);
              }
            }
          }
        });
        directoryUpdateAccounts.add(account);
      }
    }

    if (!directoryUpdateAccounts.isEmpty()) {
      directoryQueue.refreshRegisteredUsers(directoryUpdateAccounts);
    }
  }

  private boolean deviceNeedsUpdate(final Device device) {
    return device.getUninstalledFeedbackTimestamp() != 0 &&
        device.getUninstalledFeedbackTimestamp() + TimeUnit.DAYS.toMillis(2) <= Util.todayInMillis();
  }

  private boolean deviceExpired(final Device device) {
    return device.getLastSeen() + TimeUnit.DAYS.toMillis(2) <= Util.todayInMillis();
  }
}
