/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class MigrationMismatchedAccountsTableCrawler extends ManagedPeriodicWork {

  private static final Logger logger = LoggerFactory.getLogger(MigrationMismatchedAccountsTableCrawler.class);

  private static final Duration WORKER_TTL = Duration.ofMinutes(2);
  private static final Duration RUN_INTERVAL = Duration.ofMinutes(1);
  private static final String ACTIVE_WORKER_KEY = "migration_mismatched_accounts_crawler_cache_active_worker";

  private static final int MAX_BATCH_SIZE = 5_000;

  private static final Counter COMPARISONS_COUNTER = Metrics.counter(
      name(MigrationMismatchedAccountsTableCrawler.class, "comparisons"));
  private static final String MISMATCH_COUNTER_NAME = name(MigrationMismatchedAccountsTableCrawler.class, "mismatches");
  private static final Counter ERRORS_COUNTER = Metrics.counter(
      name(MigrationMismatchedAccountsTableCrawler.class, "errors"));

  private final MigrationMismatchedAccounts mismatchedAccounts;
  private final AccountsManager accountsManager;
  private final Accounts accountsDb;
  private final AccountsDynamoDb accountsDynamoDb;

  private final DynamicConfigurationManager dynamicConfigurationManager;

  public MigrationMismatchedAccountsTableCrawler(
      final MigrationMismatchedAccounts mismatchedAccounts,
      final AccountsManager accountsManager,
      final Accounts accountsDb,
      final AccountsDynamoDb accountsDynamoDb,
      final DynamicConfigurationManager dynamicConfigurationManager,
      final FaultTolerantRedisCluster cluster,
      final ScheduledExecutorService executorService) throws IOException {

    super(new ManagedPeriodicWorkLock(ACTIVE_WORKER_KEY, cluster), WORKER_TTL, RUN_INTERVAL, executorService);

    this.mismatchedAccounts = mismatchedAccounts;
    this.accountsManager = accountsManager;
    this.accountsDb = accountsDb;
    this.accountsDynamoDb = accountsDynamoDb;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public void doPeriodicWork() {

    final List<UUID> uuids = this.mismatchedAccounts.getUuids(MAX_BATCH_SIZE);

    final List<UUID> processedUuids = new ArrayList<>(uuids.size());

    try {
      for (UUID uuid : uuids) {

        try {

          final Optional<String> result = accountsManager.compareAccounts(accountsDb.get(uuid),
              accountsDynamoDb.get(uuid));

          COMPARISONS_COUNTER.increment();

          result.ifPresent(mismatchType -> {
            Metrics.counter(MISMATCH_COUNTER_NAME, "type", mismatchType)
                .increment();

            if (dynamicConfigurationManager.getConfiguration().getAccountsDynamoDbMigrationConfiguration()
                .isLogMismatches()) {
              logger.info("Mismatch: {} - {}", uuid, mismatchType);
            }
          });

          processedUuids.add(uuid);

        } catch (final Exception e) {
          ERRORS_COUNTER.increment();
          logger.warn("Failed to check account mismatch", e);
        }

      }
    } finally {
      this.mismatchedAccounts.delete(processedUuids);
    }
  }
}
