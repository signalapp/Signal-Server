package org.whispersystems.textsecuregcm.workers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.base.Optional;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.util.Base64;

import java.security.SecureRandom;

import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.ImmutableListContainerFactory;
import io.dropwizard.jdbi.ImmutableSetContainerFactory;
import io.dropwizard.jdbi.OptionalContainerFactory;
import io.dropwizard.jdbi.args.OptionalArgumentFactory;
import io.dropwizard.setup.Environment;
import redis.clients.jedis.JedisPool;

public class DeleteUserCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(DirectoryCommand.class);

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

      DataSourceFactory dbConfig = configuration.getDataSourceFactory();
      DBI               dbi      = new DBI(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword());

      dbi.registerArgumentFactory(new OptionalArgumentFactory(dbConfig.getDriverClass()));
      dbi.registerContainerFactory(new ImmutableListContainerFactory());
      dbi.registerContainerFactory(new ImmutableSetContainerFactory());
      dbi.registerContainerFactory(new OptionalContainerFactory());

      Accounts         accounts        = dbi.onDemand(Accounts.class);
      JedisPool        cacheClient     = new RedisClientFactory(configuration.getCacheConfiguration().getUrl()).getRedisClientPool();
      JedisPool        redisClient     = new RedisClientFactory(configuration.getDirectoryConfiguration().getUrl()).getRedisClientPool();
      DirectoryManager directory       = new DirectoryManager(redisClient);
      AccountsManager  accountsManager = new AccountsManager(accounts, directory, cacheClient);

      for (String user: users) {
        Optional<Account> account = accountsManager.get(user);

        if (account.isPresent()) {
          Optional<Device> device = account.get().getDevice(1);

          if (device.isPresent()) {
            byte[] random = new byte[16];
            new SecureRandom().nextBytes(random);

            device.get().setGcmId(null);
            device.get().setFetchesMessages(false);
            device.get().setAuthenticationCredentials(new AuthenticationCredentials(Base64.encodeBytes(random)));

            accountsManager.update(account.get());

            logger.warn("Removed " + account.get().getNumber());
          } else {
            logger.warn("No primary device found...");
          }
        } else {
          logger.warn("Account not found...");
        }
      }
    } catch (Exception ex) {
      logger.warn("Removal Exception", ex);
      throw new RuntimeException(ex);
    }
  }
}
