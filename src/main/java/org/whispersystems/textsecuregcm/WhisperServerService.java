/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm;

import com.google.common.base.Optional;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.db.DatabaseConfiguration;
import com.yammer.dropwizard.jdbi.DBIFactory;
import com.yammer.dropwizard.migrations.MigrationsBundle;
import com.yammer.metrics.reporting.GraphiteReporter;
import net.spy.memcached.MemcachedClient;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.skife.jdbi.v2.DBI;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.FederatedPeerAuthenticator;
import org.whispersystems.textsecuregcm.auth.MultiBasicAuthProvider;
import org.whispersystems.textsecuregcm.configuration.NexmoConfiguration;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AttachmentController;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.controllers.FederationController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.providers.MemcacheHealthCheck;
import org.whispersystems.textsecuregcm.providers.MemcachedClientFactory;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.providers.RedisHealthCheck;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.sms.NexmoSmsSender;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.PendingAccounts;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.util.UrlSigner;
import org.whispersystems.textsecuregcm.workers.DirectoryCommand;

import java.security.Security;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.JedisPool;

public class WhisperServerService extends Service<WhisperServerConfiguration> {

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  @Override
  public void initialize(Bootstrap<WhisperServerConfiguration> bootstrap) {
    bootstrap.setName("whisper-server");
    bootstrap.addCommand(new DirectoryCommand());
    bootstrap.addBundle(new MigrationsBundle<WhisperServerConfiguration>() {
      @Override
      public DatabaseConfiguration getDatabaseConfiguration(WhisperServerConfiguration configuration) {
        return configuration.getDatabaseConfiguration();
      }
    });
  }

  @Override
  public void run(WhisperServerConfiguration config, Environment environment)
      throws Exception
  {
    DBIFactory dbiFactory = new DBIFactory();
    DBI        jdbi       = dbiFactory.build(environment, config.getDatabaseConfiguration(), "postgresql");

    Accounts        accounts        = jdbi.onDemand(Accounts.class);
    PendingAccounts pendingAccounts = jdbi.onDemand(PendingAccounts.class);
    Keys            keys            = jdbi.onDemand(Keys.class);

    MemcachedClient memcachedClient = new MemcachedClientFactory(config.getMemcacheConfiguration()).getClient();
    JedisPool       redisClient     = new RedisClientFactory(config.getRedisConfiguration()).getRedisClientPool();

    DirectoryManager         directory              = new DirectoryManager(redisClient);
    PendingAccountsManager   pendingAccountsManager = new PendingAccountsManager(pendingAccounts, memcachedClient);
    AccountsManager          accountsManager        = new AccountsManager(accounts, directory, memcachedClient);
    AccountAuthenticator     accountAuthenticator   = new AccountAuthenticator(accountsManager                     );
    FederatedClientManager   federatedClientManager = new FederatedClientManager(config.getFederationConfiguration());
    RateLimiters             rateLimiters           = new RateLimiters(config.getLimitsConfiguration(), memcachedClient);
    TwilioSmsSender          twilioSmsSender        = new TwilioSmsSender(config.getTwilioConfiguration());
    Optional<NexmoSmsSender> nexmoSmsSender         = initializeNexmoSmsSender(config.getNexmoConfiguration());
    SmsSender                smsSender              = new SmsSender(twilioSmsSender, nexmoSmsSender, config.getTwilioConfiguration().isInternational());
    UrlSigner                urlSigner              = new UrlSigner(config.getS3Configuration());
    PushSender               pushSender             = new PushSender(config.getGcmConfiguration(),
                                                                     config.getApnConfiguration(),
                                                                     accountsManager, directory);

    environment.addProvider(new MultiBasicAuthProvider<>(new FederatedPeerAuthenticator(config.getFederationConfiguration()),
                                                         FederatedPeer.class,
                                                         accountAuthenticator,
                                                         Account.class, "WhisperServer"));

    environment.addResource(new AccountController(pendingAccountsManager, accountsManager, rateLimiters, smsSender));
    environment.addResource(new DirectoryController(rateLimiters, directory));
    environment.addResource(new AttachmentController(rateLimiters, federatedClientManager, urlSigner));
    environment.addResource(new KeysController(rateLimiters, keys, federatedClientManager));
    environment.addResource(new FederationController(keys, accountsManager, pushSender, urlSigner));

    environment.addServlet(new MessageController(rateLimiters, accountAuthenticator,
                                                 pushSender, federatedClientManager),
                           MessageController.PATH);

    environment.addHealthCheck(new RedisHealthCheck(redisClient));
    environment.addHealthCheck(new MemcacheHealthCheck(memcachedClient));

    environment.addProvider(new IOExceptionMapper());
    environment.addProvider(new RateLimitExceededExceptionMapper());

    if (config.getGraphiteConfiguration().isEnabled()) {
      GraphiteReporter.enable(15, TimeUnit.SECONDS,
                              config.getGraphiteConfiguration().getHost(),
                              config.getGraphiteConfiguration().getPort());
    }
  }

  private Optional<NexmoSmsSender> initializeNexmoSmsSender(NexmoConfiguration configuration) {
    if (configuration == null) {
      return Optional.absent();
    } else {
      return Optional.of(new NexmoSmsSender(configuration));
    }
  }

  public static void main(String[] args) throws Exception {
    new WhisperServerService().run(args);
  }

}
