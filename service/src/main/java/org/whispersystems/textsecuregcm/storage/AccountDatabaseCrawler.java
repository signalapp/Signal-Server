/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class AccountDatabaseCrawler implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(AccountDatabaseCrawler.class);
  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer readChunkTimer = metricRegistry.timer(name(AccountDatabaseCrawler.class, "readChunk"));
  private static final Timer processChunkTimer = metricRegistry.timer(
      name(AccountDatabaseCrawler.class, "processChunk"));

  private static final long WORKER_TTL_MS = 120_000L;
  private static final long CHUNK_INTERVAL_MILLIS = Duration.ofSeconds(2).toMillis();

  private final String name;
  private final AccountsManager accounts;
  private final int chunkSize;
  private final String workerId;
  private final AccountDatabaseCrawlerCache cache;
  private final List<AccountDatabaseCrawlerListener> listeners;

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private AtomicBoolean running = new AtomicBoolean(false);
  private boolean finished;

  public AccountDatabaseCrawler(final String name,
      AccountsManager accounts,
      AccountDatabaseCrawlerCache cache,
      List<AccountDatabaseCrawlerListener> listeners,
      int chunkSize,
      DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.name = name;
    this.accounts = accounts;
    this.chunkSize = chunkSize;
    this.workerId = UUID.randomUUID().toString();
    this.cache = cache;
    this.listeners = listeners;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
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

    while (running.get()) {
      try {
        doPeriodicWork();
        sleepWhileRunning(CHUNK_INTERVAL_MILLIS);
      } catch (Throwable t) {
        logger.warn("{}: error in database crawl: {}: {}", name, t.getClass().getSimpleName(), t.getMessage(), t);
        Util.sleep(10000);
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  public void crawlAllAccounts() {
    if (!cache.claimActiveWork(workerId, WORKER_TTL_MS)) {
      logger.info("Did not claim active work");
      return;
    }
    try {
      Optional<UUID> fromUuid = cache.getLastUuid();

      if (fromUuid.isEmpty()) {
        logger.info("{}: Started crawl", name);
        listeners.forEach(AccountDatabaseCrawlerListener::onCrawlStart);
      } else {
        logger.info("{}: Resuming crawl", name);
      }

      try {
        AccountCrawlChunk chunkAccounts;
        do {
          if (!dynamicConfigurationManager.getConfiguration().getAccountDatabaseCrawlerConfiguration()
              .crawlAllEnabled()) {
            logger.warn("Exiting crawl - not enabled by dynamic configuration");
            return;
          }
          try (Timer.Context timer = processChunkTimer.time()) {
            logger.debug("{}: Processing chunk", name);
            chunkAccounts = readChunk(fromUuid, chunkSize);

            for (AccountDatabaseCrawlerListener listener : listeners) {
              listener.timeAndProcessCrawlChunk(fromUuid, chunkAccounts.getAccounts());
            }
            fromUuid = chunkAccounts.getLastUuid();
            cacheLastUuid(fromUuid);
          }

        } while (!chunkAccounts.getAccounts().isEmpty());

        logger.info("{}: Finished crawl", name);
        listeners.forEach(AccountDatabaseCrawlerListener::onCrawlEnd);

      } catch (AccountDatabaseCrawlerRestartException e) {
        logger.warn("Crawl stopped", e);
      }

    } finally {
      cache.releaseActiveWork(workerId);
    }
  }

  @VisibleForTesting
  public void doPeriodicWork() {
    if (!dynamicConfigurationManager.getConfiguration().getAccountDatabaseCrawlerConfiguration()
        .periodicWorkEnabled()) {
      return;
    }

    if (cache.claimActiveWork(workerId, WORKER_TTL_MS)) {
      try {
        processChunk();
      } finally {
        cache.releaseActiveWork(workerId);
      }
    }
  }

  private void processChunk() {

    try (Timer.Context timer = processChunkTimer.time()) {

      final Optional<UUID> fromUuid = getLastUuid();

      if (fromUuid.isEmpty()) {
        logger.info("{}: Started crawl", name);
        listeners.forEach(AccountDatabaseCrawlerListener::onCrawlStart);
      }

      final AccountCrawlChunk chunkAccounts = readChunk(fromUuid, chunkSize);

      if (chunkAccounts.getAccounts().isEmpty()) {
        logger.info("{}: Finished crawl", name);
        listeners.forEach(AccountDatabaseCrawlerListener::onCrawlEnd);
        cacheLastUuid(Optional.empty());
      } else {
        logger.debug("{}: Processing chunk", name);
        try {
          for (AccountDatabaseCrawlerListener listener : listeners) {
            listener.timeAndProcessCrawlChunk(fromUuid, chunkAccounts.getAccounts());
          }
          cacheLastUuid(chunkAccounts.getLastUuid());
        } catch (AccountDatabaseCrawlerRestartException e) {
          cacheLastUuid(Optional.empty());
        }
      }
    }
  }

  private AccountCrawlChunk readChunk(Optional<UUID> fromUuid, int chunkSize) {
    return readChunk(fromUuid, chunkSize, readChunkTimer);
  }

  private AccountCrawlChunk readChunk(Optional<UUID> fromUuid, int chunkSize, Timer readTimer) {
    try (Timer.Context timer = readTimer.time()) {

      if (fromUuid.isPresent()) {
        return accounts.getAllFromDynamo(fromUuid.get(), chunkSize);
      }

      return accounts.getAllFromDynamo(chunkSize);
    }
  }

  private Optional<UUID> getLastUuid() {
    return cache.getLastUuid();
  }

  private void cacheLastUuid(final Optional<UUID> lastUuid) {
    cache.setLastUuid(lastUuid);
  }

  private synchronized void sleepWhileRunning(long delayMs) {
    if (running.get()) {
      Util.wait(this, delayMs);
    }
  }

}
