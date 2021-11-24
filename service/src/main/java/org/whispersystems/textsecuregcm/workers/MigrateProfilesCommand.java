/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesDynamoDb;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class MigrateProfilesCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(MigrateProfilesCommand.class);

  public MigrateProfilesCommand() {
    super(new Application<>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment) {
      }
    }, "migrate-profiles", "Migrate versioned profiles from Postgres to DynamoDB");
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-s", "--fetch-size")
        .dest("fetchSize")
        .type(Integer.class)
        .required(false)
        .setDefault(512)
        .help("The number of profiles to fetch from Postgres at once");

    subparser.addArgument("-c", "--concurrency")
        .dest("concurrency")
        .type(Integer.class)
        .required(false)
        .setDefault(64)
        .help("The maximum number of concurrent DynamoDB requests");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration) throws Exception {

    JdbiFactory jdbiFactory = new JdbiFactory();
    Jdbi accountJdbi = jdbiFactory.build(environment, configuration.getAccountsDatabaseConfiguration(), "accountdb");
    FaultTolerantDatabase accountDatabase = new FaultTolerantDatabase("account_database_delete_user", accountJdbi,
        configuration.getAccountsDatabaseConfiguration().getCircuitBreakerConfiguration());

    DynamoDbClient dynamoDbClient = DynamoDbFromConfig.client(
        configuration.getDynamoDbClientConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbFromConfig.asyncClient(
        configuration.getDynamoDbClientConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    Profiles profiles = new Profiles(accountDatabase);
    ProfilesDynamoDb profilesDynamoDb = new ProfilesDynamoDb(dynamoDbClient, dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getProfiles().getTableName());

    final int fetchSize = namespace.getInt("fetchSize");
    final int concurrency = namespace.getInt("concurrency");

    final Semaphore semaphore = new Semaphore(concurrency);

    log.info("Beginning migration");

    final AtomicInteger profilesProcessed = new AtomicInteger(0);
    final AtomicInteger profilesMigrated = new AtomicInteger(0);

    profiles.forEach((uuid, profile) -> {
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting to acquire permit");
        throw new RuntimeException(e);
      }

      profilesDynamoDb.migrate(uuid, profile)
          .whenComplete((migrated, cause) -> {
            semaphore.release();

            final int processed = profilesProcessed.incrementAndGet();

            if (cause == null) {
              if (migrated) {
                profilesMigrated.incrementAndGet();
              }
            }

            if (processed % 10_000 == 0) {
              log.info("Processed {} profiles ({} migrated)", processed, profilesMigrated.get());
            }
          });
    }, fetchSize);

    // Wait for all outstanding operations to complete
    semaphore.acquire(concurrency);
    semaphore.release(concurrency);

    log.info("Migration completed; processed {} profiles and migrated {}", profilesProcessed.get(), profilesMigrated.get());

    log.info("Removing profiles that were deleted during migration");
    final AtomicInteger profilesDeleted = new AtomicInteger(0);

    profiles.forEachDeletedProfile((uuid, version) -> {
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting to acquire permit");
        throw new RuntimeException(e);
      }

      profilesDynamoDb.delete(uuid, version)
          .whenComplete((response, cause) -> {
            semaphore.release();

            if (profilesDeleted.incrementAndGet() % 1_000 == 0) {
              log.info("Attempted to remove {} profiles", profilesDeleted.get());
            }
          });
    }, fetchSize);

    // Wait for all outstanding operations to complete
    semaphore.acquire(concurrency);
    semaphore.release(concurrency);

    log.info("Removal of deleted profiles complete; attempted to remove {} profiles", profilesDeleted.get());
  }
}
