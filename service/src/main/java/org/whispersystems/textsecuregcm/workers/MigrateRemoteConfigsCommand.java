/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jdbi.v3.core.Jdbi;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsDynamoDb;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class MigrateRemoteConfigsCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  public MigrateRemoteConfigsCommand() {
    super(new Application<>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment) {
      }
    }, "migrate-remote-config", "Migrate remote configuration from Postgres to DynamoDB");
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

    RemoteConfigs remoteConfigs = new RemoteConfigs(accountDatabase);
    RemoteConfigsDynamoDb remoteConfigsDynamoDb =
        new RemoteConfigsDynamoDb(dynamoDbClient, configuration.getDynamoDbTables().getRemoteConfig().getTableName());

    remoteConfigs.getAll().forEach(remoteConfigsDynamoDb::set);
  }
}
