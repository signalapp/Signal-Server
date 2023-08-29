/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

public class PushFeedbackProcessor extends AccountDatabaseCrawlerListener {

  private static final Logger log = LoggerFactory.getLogger(PushFeedbackProcessor.class);

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter expired = metricRegistry.meter(name(getClass(), "unregistered", "expired"));
  private final Meter recovered = metricRegistry.meter(name(getClass(), "unregistered", "recovered"));

  private static final Counter UPDATED_ACCOUNT_COUNTER = Metrics.counter(
      MetricsUtil.name(PushFeedbackProcessor.class, "updatedAccounts"));


  private final AccountsManager accountsManager;
  private final ExecutorService updateExecutor;

  public PushFeedbackProcessor(AccountsManager accountsManager, ExecutorService updateExecutor) {
    this.accountsManager = accountsManager;
    this.updateExecutor = updateExecutor;
  }

  @Override
  public void onCrawlStart() {}

  @Override
  public void onCrawlEnd() {
  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) {

    final List<CompletableFuture<Void>> updateFutures = chunkAccounts.stream()
        .filter(account -> {
          boolean update = false;

          for (Device device : account.getDevices()) {
            if (deviceNeedsUpdate(device)) {
              if (deviceExpired(device)) {
                if (device.isEnabled()) {
                  expired.mark();
                  update = true;
                }
              } else {
                recovered.mark();
                update = true;
              }
            }
          }

          return update;
        })
        .map(account -> CompletableFuture.runAsync(() -> {
              // fetch a new version, since the chunk is shared and implicitly read-only
              accountsManager.getByAccountIdentifier(account.getUuid()).ifPresent(accountToUpdate -> {
                accountsManager.update(accountToUpdate, a -> {
                  for (Device device : a.getDevices()) {
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
              });
            }, updateExecutor)
            .whenComplete((ignored, throwable) -> {
              if (throwable != null) {
                log.warn("Failed to update account {}", account.getUuid(), throwable);
              } else {
                UPDATED_ACCOUNT_COUNTER.increment();
              }
            }))
        .toList();

    try {
      CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0]))
          .orTimeout(10, TimeUnit.MINUTES)
          .join();
    } catch (final Exception e) {
      log.debug("Failed to update one or more accounts in chunk", e);
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
