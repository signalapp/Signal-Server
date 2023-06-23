/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class AccountDatabaseCrawler {

  private static final Logger logger = LoggerFactory.getLogger(AccountDatabaseCrawler.class);
  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer readChunkTimer = metricRegistry.timer(name(AccountDatabaseCrawler.class, "readChunk"));
  private static final Timer processChunkTimer = metricRegistry.timer(
      name(AccountDatabaseCrawler.class, "processChunk"));

  private static final long WORKER_TTL_MS = 120_000L;

  private final String name;
  private final AccountsManager accounts;
  private final int chunkSize;
  private final String workerId;
  private final AccountDatabaseCrawlerCache cache;
  private final List<AccountDatabaseCrawlerListener> listeners;

  public AccountDatabaseCrawler(final String name,
      AccountsManager accounts,
      AccountDatabaseCrawlerCache cache,
      List<AccountDatabaseCrawlerListener> listeners,
      int chunkSize) {
    this.name = name;
    this.accounts = accounts;
    this.chunkSize = chunkSize;
    this.workerId = UUID.randomUUID().toString();
    this.cache = cache;
    this.listeners = listeners;
  }

  public void crawlAllAccounts() {
    if (!cache.claimActiveWork(workerId, WORKER_TTL_MS)) {
      logger.info("Did not claim active work");
      return;
    }
    try {
      Optional<UUID> fromUuid = getLastUuid();

      if (fromUuid.isEmpty()) {
        logger.info("{}: Started crawl", name);
        listeners.forEach(AccountDatabaseCrawlerListener::onCrawlStart);
      } else {
        logger.info("{}: Resuming crawl", name);
      }

      AccountCrawlChunk chunkAccounts;
      do {
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

    } finally {
      cache.releaseActiveWork(workerId);
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

}
