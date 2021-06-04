/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

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

public class MigrationRetryAccountsTableCrawler extends ManagedPeriodicWork {

  private static final Logger logger = LoggerFactory.getLogger(MigrationRetryAccountsTableCrawler.class);

  private static final Duration WORKER_TTL = Duration.ofMinutes(2);
  private static final Duration RUN_INTERVAL = Duration.ofMinutes(15);
  private static final String ACTIVE_WORKER_KEY = "migration_retry_accounts_crawler_cache_active_worker";

  private static final int MAX_BATCH_SIZE = 5_000;

  private static final Counter MIGRATED_COUNTER = Metrics.counter(name(MigrationRetryAccountsTableCrawler.class, "migrated"));
  private static final Counter ERROR_COUNTER = Metrics.counter(name(MigrationRetryAccountsTableCrawler.class, "error"));
  private static final Counter TOTAL_COUNTER = Metrics.counter(name(MigrationRetryAccountsTableCrawler.class, "total"));

  private final MigrationRetryAccounts retryAccounts;
  private final AccountsManager accountsManager;
  private final AccountsDynamoDb accountsDynamoDb;

  public MigrationRetryAccountsTableCrawler(
      final MigrationRetryAccounts retryAccounts,
      final AccountsManager accountsManager,
      final AccountsDynamoDb accountsDynamoDb,
      final FaultTolerantRedisCluster cluster,
      final ScheduledExecutorService executorService) throws IOException {

    super(new ManagedPeriodicWorkLock(ACTIVE_WORKER_KEY, cluster), WORKER_TTL, RUN_INTERVAL, executorService);

    this.retryAccounts = retryAccounts;
    this.accountsManager = accountsManager;
    this.accountsDynamoDb = accountsDynamoDb;
  }

  @Override
  public void doPeriodicWork() {

    final List<UUID> uuids = this.retryAccounts.getUuids(MAX_BATCH_SIZE);

    final List<UUID> processedUuids = new ArrayList<>(uuids.size());

    try {
      for (UUID uuid : uuids) {

        try {
          final Optional<Account> maybeDynamoAccount = accountsDynamoDb.get(uuid);

          if (maybeDynamoAccount.isEmpty()) {
            accountsManager.get(uuid).ifPresent(account -> {
              accountsDynamoDb.migrate(account);
              MIGRATED_COUNTER.increment();
            });
          }

          processedUuids.add(uuid);

          TOTAL_COUNTER.increment();

        } catch (final Exception e) {
          ERROR_COUNTER.increment();
          logger.warn("Failed to migrate account");
        }

      }
    } finally {
      this.retryAccounts.delete(processedUuids);
    }
  }
}
