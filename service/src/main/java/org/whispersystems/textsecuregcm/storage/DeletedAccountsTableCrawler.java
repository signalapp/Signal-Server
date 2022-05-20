/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest.User;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Pair;

public class DeletedAccountsTableCrawler extends ManagedPeriodicWork {

  private static final Duration WORKER_TTL = Duration.ofMinutes(2);
  private static final Duration RUN_INTERVAL = Duration.ofMinutes(15);
  private static final String ACTIVE_WORKER_KEY = "deleted_accounts_crawler_cache_active_worker";

  private static final int MAX_BATCH_SIZE = 5_000;
  private static final String BATCH_SIZE_DISTRIBUTION_NAME = name(DeletedAccountsTableCrawler.class, "batchSize");

  private final DeletedAccountsManager deletedAccountsManager;
  private final List<DeletedAccountsDirectoryReconciler> reconcilers;

  public DeletedAccountsTableCrawler(
      final DeletedAccountsManager deletedAccountsManager,
      final List<DeletedAccountsDirectoryReconciler> reconcilers,
      final FaultTolerantRedisCluster cluster,
      final ScheduledExecutorService executorService) throws IOException {

    super(new ManagedPeriodicWorkLock(ACTIVE_WORKER_KEY, cluster), WORKER_TTL, RUN_INTERVAL, executorService);

    this.deletedAccountsManager = deletedAccountsManager;
    this.reconcilers = reconcilers;
  }

  @Override
  public void doPeriodicWork() throws Exception {

    deletedAccountsManager.lockAndReconcileAccounts(MAX_BATCH_SIZE, deletedAccounts -> {
      final List<User> deletedUsers = deletedAccounts.stream()
          .map(pair -> new User(pair.first(), pair.second()))
          .collect(Collectors.toList());

      for (DeletedAccountsDirectoryReconciler reconciler : reconcilers) {
        reconciler.onCrawlChunk(deletedUsers);
      }

      final List<String> reconciledPhoneNumbers = deletedAccounts.stream()
          .map(Pair::second)
          .collect(Collectors.toList());

      Metrics.summary(BATCH_SIZE_DISTRIBUTION_NAME).record(reconciledPhoneNumbers.size());

      return reconciledPhoneNumbers;
    });
  }

}
