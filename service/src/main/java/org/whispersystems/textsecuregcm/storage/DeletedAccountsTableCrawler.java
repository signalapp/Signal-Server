/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest.User;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;

public class DeletedAccountsTableCrawler implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DeletedAccountsTableCrawler.class);

  private static final Duration WORKER_TTL = Duration.ofMinutes(2);
  private static final Duration RUN_INTERVAL = Duration.ofMinutes(15);
  private static final int MAX_BATCH_SIZE = 5_000;

  private static final String BATCH_SIZE_DISTRIBUTION_NAME = name(DeletedAccountsTableCrawler.class, "batchSize");

  private final DeletedAccounts deletedAccounts;
  private final DeletedAccountsTableCrawlerCache cache;
  private final List<DeletedAccountsDirectoryReconciler> reconcilers;
  private final String workerId;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private boolean finished;

  public DeletedAccountsTableCrawler(
      final DeletedAccounts deletedAccounts,
      final DeletedAccountsTableCrawlerCache cache,
      final List<DeletedAccountsDirectoryReconciler> reconcilers) {

    this.deletedAccounts = deletedAccounts;
    this.cache = cache;
    this.reconcilers = reconcilers;
    this.workerId = UUID.randomUUID().toString();
  }

  @Override
  public void start() throws Exception {
    running.set(true);
    new Thread(this).start();
  }

  @Override
  public void stop() throws Exception {

    running.set(false);
    notifyAll();

    while (!finished) {
      Util.wait(this);
    }
  }

  @Override
  public void run() {

    while(running.get()) {
      try {
        doPeriodicWork();
        sleepWhileRunning(RUN_INTERVAL);
      } catch (final Exception e) {
        logger.warn("Error in crawl crawl", e);

        // wait a bit, in case the error is caused by external instability
        Util.sleep(10_000);
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  private void doPeriodicWork() {

    // generic
    if (cache.claimActiveWork(workerId, WORKER_TTL)) {

      try {
        final long startTimeMs = System.currentTimeMillis();

        // specific
        final List<Pair<UUID, String>> deletedAccounts = this.deletedAccounts.list(MAX_BATCH_SIZE);

        final List<User> deletedUsers = deletedAccounts.stream()
            .map(pair -> new User(pair.first(), pair.second()))
            .collect(Collectors.toList());

        for (DeletedAccountsDirectoryReconciler reconciler : reconcilers) {
          reconciler.onCrawlChunk(deletedUsers);
        }

        final List<UUID> deletedUuids = deletedAccounts.stream()
            .map(Pair::first)
            .collect(Collectors.toList());

        this.deletedAccounts.delete(deletedUuids);

        DistributionSummary.builder(BATCH_SIZE_DISTRIBUTION_NAME)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
            .record(deletedUuids.size());

        // generic
        final long endTimeMs = System.currentTimeMillis();
        final Duration sleepInterval = RUN_INTERVAL.minusMillis(endTimeMs - startTimeMs);
        if (sleepInterval.getSeconds() > 0) {
          sleepWhileRunning(sleepInterval);
        }

      } catch (final Exception e) {
        logger.warn("Failed to process chunk", e);

        // wait a full interval for recovery
        sleepWhileRunning(RUN_INTERVAL);

      } finally {
        cache.releaseActiveWork(workerId);
      }
    }
  }

  private synchronized void sleepWhileRunning(Duration delay) {
    if (running.get()) Util.wait(this, delay.toMillis());
  }
}
