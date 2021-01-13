/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.Messages;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.codahale.metrics.MetricRegistry.name;

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
      Jdbi                  messageJdbi                 = jdbiFactory.build(environment, configuration.getMessageStoreConfiguration(), "messagedb" );
      FaultTolerantDatabase accountDatabase             = new FaultTolerantDatabase("account_database_delete_user", accountJdbi, configuration.getAccountsDatabaseConfiguration().getCircuitBreakerConfiguration());
      FaultTolerantDatabase messageDatabase             = new FaultTolerantDatabase("message_database", messageJdbi, configuration.getMessageStoreConfiguration().getCircuitBreakerConfiguration());
      ClientResources       redisClusterClientResources = ClientResources.builder().build();


      FaultTolerantRedisCluster cacheCluster = new FaultTolerantRedisCluster("main_cache_cluster", configuration.getCacheClusterConfiguration(), redisClusterClientResources);

      ExecutorService keyspaceNotificationDispatchExecutor = environment.lifecycle().executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(4).build();

      Accounts                  accounts             = new Accounts(accountDatabase);
      Usernames                 usernames            = new Usernames(accountDatabase);
      Profiles                  profiles             = new Profiles(accountDatabase);
      ReservedUsernames         reservedUsernames    = new ReservedUsernames(accountDatabase);
      Keys                      keys                 = new Keys(accountDatabase, configuration.getAccountsDatabaseConfiguration().getKeyOperationRetryConfiguration());
      Messages                  messages             = new Messages(messageDatabase);
      ReplicatedJedisPool       redisClient          = new RedisClientFactory("directory_cache_delete_command", configuration.getDirectoryConfiguration().getRedisConfiguration().getUrl(), configuration.getDirectoryConfiguration().getRedisConfiguration().getReplicaUrls(), configuration.getDirectoryConfiguration().getRedisConfiguration().getCircuitBreakerConfiguration()).getRedisClientPool();
      FaultTolerantRedisCluster messagesCacheCluster = new FaultTolerantRedisCluster("messages_cluster", configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
      FaultTolerantRedisCluster metricsCluster       = new FaultTolerantRedisCluster("metrics_cluster", configuration.getMetricsClusterConfiguration(), redisClusterClientResources);
      MessagesCache             messagesCache        = new MessagesCache(messagesCacheCluster, keyspaceNotificationDispatchExecutor);
      PushLatencyManager        pushLatencyManager   = new PushLatencyManager(metricsCluster);
      DirectoryQueue            directoryQueue       = new DirectoryQueue  (configuration.getDirectoryConfiguration().getSqsConfiguration());
      DirectoryManager          directory            = new DirectoryManager(redisClient                                                    );
      UsernamesManager          usernamesManager     = new UsernamesManager(usernames, reservedUsernames, cacheCluster);
      ProfilesManager           profilesManager      = new ProfilesManager(profiles, cacheCluster);
      MessagesManager           messagesManager      = new MessagesManager(messages, messagesCache, pushLatencyManager);
      AccountsManager           accountsManager      = new AccountsManager(accounts, directory, cacheCluster, directoryQueue, keys, messagesManager, usernamesManager, profilesManager);

      for (String user: users) {
        Optional<Account> account = accountsManager.get(user);

        if (account.isPresent()) {
          accountsManager.delete(account.get(), AccountsManager.DeletionReason.ADMIN_DELETED);
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
