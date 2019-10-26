/*
 * Copyright (C) 2018 Open WhisperSystems
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.lifecycle.Managed;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class AccountDatabaseCrawler implements Managed, Runnable {

  private static final Logger         logger         = LoggerFactory.getLogger(AccountDatabaseCrawler.class);
  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer = metricRegistry.timer(name(AccountDatabaseCrawler.class, "readChunk"));

  private static final long   WORKER_TTL_MS              = 120_000L;
  private static final long   ACCELERATED_CHUNK_INTERVAL = 10L;

  private final AccountsManager                      accounts;
  private final int                                  chunkSize;
  private final long                                 chunkIntervalMs;
  private final String                               workerId;
  private final AccountDatabaseCrawlerCache          cache;
  private final List<AccountDatabaseCrawlerListener> listeners;

  private AtomicBoolean running = new AtomicBoolean(false);
  private boolean finished;

  public AccountDatabaseCrawler(AccountsManager accounts,
                                AccountDatabaseCrawlerCache cache,
                                List<AccountDatabaseCrawlerListener> listeners,
                                int chunkSize,
                                long chunkIntervalMs)
  {
    this.accounts             = accounts;
    this.chunkSize            = chunkSize;
    this.chunkIntervalMs      = chunkIntervalMs;
    this.workerId             = UUID.randomUUID().toString();
    this.cache                = cache;
    this.listeners            = listeners;
  }

  @Override
  public synchronized void start() {
    running.set(true);
    new Thread(this).start();
  }

  @Override
  public synchronized void stop() {
    running.set(false);
    notifyAll();
    while (!finished) {
      Util.wait(this);
    }
  }

  @Override
  public void run() {
    boolean accelerated = false;

    while (running.get()) {
      try {
        accelerated = doPeriodicWork();
        sleepWhileRunning(accelerated ? ACCELERATED_CHUNK_INTERVAL : chunkIntervalMs);
      } catch (Throwable t) {
        logger.warn("error in database crawl: ", t);
        Util.sleep(10000);
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  @VisibleForTesting
  public boolean doPeriodicWork() {
    if (cache.claimActiveWork(workerId, WORKER_TTL_MS)) {
      try {
        long startTimeMs = System.currentTimeMillis();
        processChunk();
        if (cache.isAccelerated()) {
          return true;
        }
        long endTimeMs = System.currentTimeMillis();
        long sleepIntervalMs = chunkIntervalMs - (endTimeMs - startTimeMs);
        if (sleepIntervalMs > 0) sleepWhileRunning(sleepIntervalMs);
      } finally {
        cache.releaseActiveWork(workerId);
      }
    }
    return false;
  }

  private void processChunk() {
    Optional<UUID> fromUuid = cache.getLastUuid();

    if (!fromUuid.isPresent()) {
      listeners.forEach(AccountDatabaseCrawlerListener::onCrawlStart);
    }

    List<Account> chunkAccounts = readChunk(fromUuid, chunkSize);

    if (chunkAccounts.isEmpty()) {
      listeners.forEach(listener -> listener.onCrawlEnd(fromUuid));
      cache.setLastUuid(Optional.empty());
      cache.clearAccelerate();
    } else {
      try {
        for (AccountDatabaseCrawlerListener listener : listeners) {
          listener.timeAndProcessCrawlChunk(fromUuid, chunkAccounts);
        }
        cache.setLastUuid(Optional.of(chunkAccounts.get(chunkAccounts.size() - 1).getUuid()));
      } catch (AccountDatabaseCrawlerRestartException e) {
        cache.setLastUuid(Optional.empty());
        cache.clearAccelerate();
      }

    }

  }

  private List<Account> readChunk(Optional<UUID> fromUuid, int chunkSize) {
    try (Timer.Context timer = readChunkTimer.time()) {
      List<Account> chunkAccounts;

      if (fromUuid.isPresent()) {
        chunkAccounts = accounts.getAllFrom(fromUuid.get(), chunkSize);
      } else {
        chunkAccounts = accounts.getAllFrom(chunkSize);
      }

      return chunkAccounts;
    }
  }

  private synchronized void sleepWhileRunning(long delayMs) {
    if (running.get()) Util.wait(this, delayMs);
  }

}
