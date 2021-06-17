/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
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

  private final DeletedAccounts deletedAccounts;
  private final List<DeletedAccountsDirectoryReconciler> reconcilers;

  public DeletedAccountsTableCrawler(
      final DeletedAccounts deletedAccounts,
      final List<DeletedAccountsDirectoryReconciler> reconcilers,
      final FaultTolerantRedisCluster cluster) throws IOException {

    super(new ManagedPeriodicWorkCache(ACTIVE_WORKER_KEY, cluster), WORKER_TTL, RUN_INTERVAL);

    this.deletedAccounts = deletedAccounts;
    this.reconcilers = reconcilers;
  }

  @Override
  public void doPeriodicWork() throws Exception {

    final List<Pair<UUID, String>> deletedAccounts = this.deletedAccounts.listAccountsToReconcile(MAX_BATCH_SIZE);

    final List<User> deletedUsers = deletedAccounts.stream()
        .map(pair -> new User(pair.first(), pair.second()))
        .collect(Collectors.toList());

    for (DeletedAccountsDirectoryReconciler reconciler : reconcilers) {
      reconciler.onCrawlChunk(deletedUsers);
    }

    final List<String> reconciledPhoneNumbers = deletedAccounts.stream()
        .map(Pair::second)
        .collect(Collectors.toList());

    this.deletedAccounts.markReconciled(reconciledPhoneNumbers);

    DistributionSummary.builder(BATCH_SIZE_DISTRIBUTION_NAME)
        .publishPercentileHistogram()
        .register(Metrics.globalRegistry)
        .record(reconciledPhoneNumbers.size());
  }

}
