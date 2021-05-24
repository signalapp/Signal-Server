/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static com.codahale.metrics.MetricRegistry.name;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Metrics;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsDynamoDb;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.AccountsManager.DeletionReason;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.MigrationDeletedAccounts;
import org.whispersystems.textsecuregcm.storage.MigrationRetryAccounts;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageDynamoDb;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DeleteUserCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(DeleteUserCommand.class);

  public DeleteUserCommand() {
    super(new Application<WhisperServerConfiguration>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment)
          throws Exception
      {

      }
    }, "rmuser", "remove user");
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);
    subparser.addArgument("-u", "--user")
             .dest("user")
             .type(String.class)
             .required(true)
             .help("The user to remove");
  }

  @Override
  protected void run(Environment environment, Namespace namespace,
                     WhisperServerConfiguration configuration)
      throws Exception
  {
    try {
      String[] users = namespace.getString("user").split(",");

      environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      JdbiFactory           jdbiFactory                 = new JdbiFactory();
      Jdbi                  accountJdbi                 = jdbiFactory.build(environment, configuration.getAccountsDatabaseConfiguration(), "accountdb");
      FaultTolerantDatabase accountDatabase             = new FaultTolerantDatabase("account_database_delete_user", accountJdbi, configuration.getAccountsDatabaseConfiguration().getCircuitBreakerConfiguration());
      ClientResources       redisClusterClientResources = ClientResources.builder().build();

      ThreadPoolExecutor accountsDynamoDbMigrationThreadPool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
          new LinkedBlockingDeque<>());

      DynamoDbClient reportMessagesDynamoDb = DynamoDbFromConfig.client(configuration.getReportMessageDynamoDbConfiguration(),
          software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());
      DynamoDbClient messageDynamoDb = DynamoDbFromConfig.client(configuration.getMessageDynamoDbConfiguration(),
          software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());
      DynamoDbClient preKeysDynamoDb = DynamoDbFromConfig.client(configuration.getKeysDynamoDbConfiguration(),
          software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());
      DynamoDbClient accountsDynamoDbClient = DynamoDbFromConfig.client(configuration.getAccountsDynamoDbConfiguration(),
          software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());
      DynamoDbAsyncClient accountsDynamoDbAsyncClient = DynamoDbFromConfig.asyncClient(configuration.getAccountsDynamoDbConfiguration(),
          software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create(),
          accountsDynamoDbMigrationThreadPool);

      FaultTolerantRedisCluster cacheCluster = new FaultTolerantRedisCluster("main_cache_cluster", configuration.getCacheClusterConfiguration(), redisClusterClientResources);

      ExecutorService keyspaceNotificationDispatchExecutor = environment.lifecycle().executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(4).build();
      ExecutorService backupServiceExecutor = environment.lifecycle().executorService(name(getClass(), "backupService-%d")).maxThreads(8).minThreads(1).build();
      ExecutorService storageServiceExecutor = environment.lifecycle().executorService(name(getClass(), "storageService-%d")).maxThreads(8).minThreads(1).build();

      ExternalServiceCredentialGenerator backupCredentialsGenerator = new ExternalServiceCredentialGenerator(configuration.getSecureBackupServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);
      ExternalServiceCredentialGenerator storageCredentialsGenerator = new ExternalServiceCredentialGenerator(configuration.getSecureStorageServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);

      DynamicConfigurationManager dynamicConfigurationManager = new DynamicConfigurationManager(configuration.getAppConfig().getApplication(), configuration.getAppConfig().getEnvironment(), configuration.getAppConfig().getConfigurationName());
      dynamicConfigurationManager.start();

      ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

      DynamoDbClient migrationDeletedAccountsDynamoDb = DynamoDbFromConfig.client(configuration.getMigrationDeletedAccountsDynamoDbConfiguration(),
          software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());
      DynamoDbClient migrationRetryAccountsDynamoDb = DynamoDbFromConfig.client(configuration.getMigrationRetryAccountsDynamoDbConfiguration(),
          software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

      MigrationDeletedAccounts migrationDeletedAccounts = new MigrationDeletedAccounts(migrationDeletedAccountsDynamoDb, configuration.getMigrationDeletedAccountsDynamoDbConfiguration().getTableName());
      MigrationRetryAccounts migrationRetryAccounts = new MigrationRetryAccounts(migrationRetryAccountsDynamoDb, configuration.getMigrationRetryAccountsDynamoDbConfiguration().getTableName());

      Accounts                  accounts             = new Accounts(accountDatabase);
      AccountsDynamoDb          accountsDynamoDb     = new AccountsDynamoDb(accountsDynamoDbClient, accountsDynamoDbAsyncClient, accountsDynamoDbMigrationThreadPool, configuration.getAccountsDynamoDbConfiguration().getTableName(), configuration.getAccountsDynamoDbConfiguration().getPhoneNumberTableName(), migrationDeletedAccounts, migrationRetryAccounts);
      Usernames                 usernames            = new Usernames(accountDatabase);
      Profiles                  profiles             = new Profiles(accountDatabase);
      ReservedUsernames         reservedUsernames    = new ReservedUsernames(accountDatabase);
      KeysDynamoDb              keysDynamoDb         = new KeysDynamoDb(preKeysDynamoDb, configuration.getKeysDynamoDbConfiguration().getTableName());
      MessagesDynamoDb          messagesDynamoDb     = new MessagesDynamoDb(messageDynamoDb, configuration.getMessageDynamoDbConfiguration().getTableName(), configuration.getMessageDynamoDbConfiguration().getTimeToLive());
      FaultTolerantRedisCluster messageInsertCacheCluster = new FaultTolerantRedisCluster("message_insert_cluster", configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
      FaultTolerantRedisCluster messageReadDeleteCluster = new FaultTolerantRedisCluster("message_read_delete_cluster", configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
      FaultTolerantRedisCluster metricsCluster       = new FaultTolerantRedisCluster("metrics_cluster", configuration.getMetricsClusterConfiguration(), redisClusterClientResources);
      SecureBackupClient        secureBackupClient   = new SecureBackupClient(backupCredentialsGenerator, backupServiceExecutor, configuration.getSecureBackupServiceConfiguration());
      SecureStorageClient       secureStorageClient  = new SecureStorageClient(storageCredentialsGenerator, storageServiceExecutor, configuration.getSecureStorageServiceConfiguration());
      MessagesCache             messagesCache        = new MessagesCache(messageInsertCacheCluster, messageReadDeleteCluster, keyspaceNotificationDispatchExecutor);
      PushLatencyManager        pushLatencyManager   = new PushLatencyManager(metricsCluster);
      DirectoryQueue            directoryQueue       = new DirectoryQueue  (configuration.getDirectoryConfiguration().getSqsConfiguration());
      UsernamesManager          usernamesManager     = new UsernamesManager(usernames, reservedUsernames, cacheCluster);
      ProfilesManager           profilesManager      = new ProfilesManager(profiles, cacheCluster);
      ReportMessageDynamoDb     reportMessageDynamoDb = new ReportMessageDynamoDb(reportMessagesDynamoDb, configuration.getReportMessageDynamoDbConfiguration().getTableName());
      ReportMessageManager      reportMessageManager = new ReportMessageManager(reportMessageDynamoDb, Metrics.globalRegistry);
      MessagesManager           messagesManager      = new MessagesManager(messagesDynamoDb, messagesCache, pushLatencyManager, reportMessageManager);
      AccountsManager           accountsManager      = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);

      for (String user: users) {
        Optional<Account> account = accountsManager.get(user);

        if (account.isPresent()) {
          accountsManager.delete(account.get(), DeletionReason.ADMIN_DELETED);
          logger.warn("Removed " + account.get().getNumber());
        } else {
          logger.warn("Account not found");
        }
      }
    } catch (Exception ex) {
      logger.warn("Removal Exception", ex);
      throw new RuntimeException(ex);
    }
  }
}
