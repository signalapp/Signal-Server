package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class AccountsDynamoDbMigrator extends AccountDatabaseCrawlerListener {

  private static final Counter MIGRATED_COUNTER = Metrics.counter(name(AccountsDynamoDbMigrator.class, "migrated"));
  private static final Counter ERROR_COUNTER = Metrics.counter(name(AccountsDynamoDbMigrator.class, "error"));

  private final AccountsDynamoDb accountsDynamoDb;
  private final DynamicConfigurationManager dynamicConfigurationManager;

  public AccountsDynamoDbMigrator(final AccountsDynamoDb accountsDynamoDb, final DynamicConfigurationManager dynamicConfigurationManager) {
    this.accountsDynamoDb = accountsDynamoDb;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public void onCrawlStart() {

  }

  @Override
  public void onCrawlEnd(Optional<UUID> fromUuid) {

  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) {

    if (!dynamicConfigurationManager.getConfiguration().getAccountsDynamoDbMigrationConfiguration().isBackgroundMigrationEnabled()) {
      return;
    }

    for (Account account : chunkAccounts) {
      try {
        final boolean migrated = accountsDynamoDb.migrate(account);
        if (migrated) {
          MIGRATED_COUNTER.increment();
        }
      } catch (final Exception e) {

        ERROR_COUNTER.increment();
      }
    }
  }
}
