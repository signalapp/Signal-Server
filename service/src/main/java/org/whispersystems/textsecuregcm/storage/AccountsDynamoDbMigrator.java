package org.whispersystems.textsecuregcm.storage;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class AccountsDynamoDbMigrator extends AccountDatabaseCrawlerListener {

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

    if (!dynamicConfigurationManager.getConfiguration().getAccountsDynamoDbMigrationConfiguration()
        .isBackgroundMigrationEnabled()) {
      return;
    }

    final CompletableFuture<Void> migrationBatch = accountsDynamoDb.migrate(chunkAccounts,
        dynamicConfigurationManager.getConfiguration().getAccountsDynamoDbMigrationConfiguration().getBackgroundMigrationExecutorThreads());

    migrationBatch.join();
  }
}
