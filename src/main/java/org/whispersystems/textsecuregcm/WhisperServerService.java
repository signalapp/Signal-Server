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

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.base.Optional;
import net.spy.memcached.MemcachedClient;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.skife.jdbi.v2.DBI;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.FederatedPeerAuthenticator;
import org.whispersystems.textsecuregcm.auth.MultiBasicAuthProvider;
import org.whispersystems.textsecuregcm.configuration.NexmoConfiguration;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AttachmentController;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.controllers.FederationControllerV1;
import org.whispersystems.textsecuregcm.controllers.FederationControllerV2;
import org.whispersystems.textsecuregcm.controllers.KeysControllerV1;
import org.whispersystems.textsecuregcm.controllers.KeysControllerV2;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.ReceiptController;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.CpuUsageGauge;
import org.whispersystems.textsecuregcm.metrics.FreeMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.JsonMetricsReporter;
import org.whispersystems.textsecuregcm.metrics.NetworkReceivedGauge;
import org.whispersystems.textsecuregcm.metrics.NetworkSentGauge;
import org.whispersystems.textsecuregcm.providers.MemcacheHealthCheck;
import org.whispersystems.textsecuregcm.providers.MemcachedClientFactory;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.providers.RedisHealthCheck;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.WebsocketSender;
import org.whispersystems.textsecuregcm.sms.NexmoSmsSender;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.PendingAccounts;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingDevices;
import org.whispersystems.textsecuregcm.storage.PendingDevicesManager;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.StoredMessages;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.UrlSigner;
import org.whispersystems.textsecuregcm.websocket.WebsocketControllerFactory;
import org.whispersystems.textsecuregcm.workers.DirectoryCommand;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletRegistration;
import java.security.Security;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.Application;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.metrics.graphite.GraphiteReporterFactory;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import redis.clients.jedis.JedisPool;

public class WhisperServerService extends Application<WhisperServerConfiguration> {

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  @Override
  public void initialize(Bootstrap<WhisperServerConfiguration> bootstrap) {
    bootstrap.addCommand(new DirectoryCommand());
    bootstrap.addBundle(new MigrationsBundle<WhisperServerConfiguration>() {
      @Override
      public DataSourceFactory getDataSourceFactory(WhisperServerConfiguration configuration) {
        return configuration.getDataSourceFactory();
      }
    });
  }

  @Override
  public String getName() {
    return "whisper-server";
  }

  @Override
  public void run(WhisperServerConfiguration config, Environment environment)
      throws Exception
  {
    SharedMetricRegistries.add(Constants.METRICS_NAME, environment.metrics());
    environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    DBIFactory dbiFactory = new DBIFactory();
    DBI        jdbi       = dbiFactory.build(environment, config.getDataSourceFactory(), "postgresql");

    Accounts        accounts        = jdbi.onDemand(Accounts.class);
    PendingAccounts pendingAccounts = jdbi.onDemand(PendingAccounts.class);
    PendingDevices  pendingDevices  = jdbi.onDemand(PendingDevices.class);
    Keys            keys            = jdbi.onDemand(Keys.class);

    MemcachedClient memcachedClient = new MemcachedClientFactory(config.getMemcacheConfiguration()).getClient();
    JedisPool       redisClient     = new RedisClientFactory(config.getRedisConfiguration()).getRedisClientPool();

    DirectoryManager       directory              = new DirectoryManager(redisClient);
    PendingAccountsManager pendingAccountsManager = new PendingAccountsManager(pendingAccounts, memcachedClient);
    PendingDevicesManager  pendingDevicesManager  = new PendingDevicesManager (pendingDevices, memcachedClient );
    AccountsManager        accountsManager        = new AccountsManager(accounts, directory, memcachedClient);
    FederatedClientManager federatedClientManager = new FederatedClientManager(config.getFederationConfiguration());
    StoredMessages         storedMessages         = new StoredMessages(redisClient);
    PubSubManager          pubSubManager          = new PubSubManager(redisClient);

    APNSender apnSender = new APNSender(accountsManager, pubSubManager, storedMessages, memcachedClient,
                                        config.getApnConfiguration().getCertificate(),
                                        config.getApnConfiguration().getKey());

    GCMSender gcmSender = new GCMSender(accountsManager,
                                        config.getGcmConfiguration().getSenderId(),
                                        config.getGcmConfiguration().getApiKey());

    WebsocketSender websocketSender = new WebsocketSender(storedMessages, pubSubManager);

    environment.lifecycle().manage(apnSender);
    environment.lifecycle().manage(gcmSender);

    AccountAuthenticator     deviceAuthenticator    = new AccountAuthenticator(accountsManager);
    RateLimiters             rateLimiters           = new RateLimiters(config.getLimitsConfiguration(), memcachedClient);

    TwilioSmsSender          twilioSmsSender        = new TwilioSmsSender(config.getTwilioConfiguration());
    Optional<NexmoSmsSender> nexmoSmsSender         = initializeNexmoSmsSender(config.getNexmoConfiguration());
    SmsSender                smsSender              = new SmsSender(twilioSmsSender, nexmoSmsSender, config.getTwilioConfiguration().isInternational());
    UrlSigner                urlSigner              = new UrlSigner(config.getS3Configuration());
    PushSender               pushSender             = new PushSender(gcmSender, apnSender, websocketSender);

    AttachmentController attachmentController = new AttachmentController(rateLimiters, federatedClientManager, urlSigner);
    KeysControllerV1     keysControllerV1     = new KeysControllerV1(rateLimiters, keys, accountsManager, federatedClientManager);
    KeysControllerV2     keysControllerV2     = new KeysControllerV2(rateLimiters, keys, accountsManager, federatedClientManager);
    MessageController    messageController    = new MessageController(rateLimiters, pushSender, accountsManager, federatedClientManager);

    environment.jersey().register(new MultiBasicAuthProvider<>(new FederatedPeerAuthenticator(config.getFederationConfiguration()),
                                                               FederatedPeer.class,
                                                               deviceAuthenticator,
                                                               Device.class, "WhisperServer"));

    environment.jersey().register(new AccountController(pendingAccountsManager, accountsManager, rateLimiters, smsSender, storedMessages));
    environment.jersey().register(new DeviceController(pendingDevicesManager, accountsManager, rateLimiters));
    environment.jersey().register(new DirectoryController(rateLimiters, directory));
    environment.jersey().register(new FederationControllerV1(accountsManager, attachmentController, messageController, keysControllerV1));
    environment.jersey().register(new FederationControllerV2(accountsManager, attachmentController, messageController, keysControllerV2));
    environment.jersey().register(new ReceiptController(accountsManager, federatedClientManager, pushSender));
    environment.jersey().register(attachmentController);
    environment.jersey().register(keysControllerV1);
    environment.jersey().register(keysControllerV2);
    environment.jersey().register(messageController);

    if (config.getWebsocketConfiguration().isEnabled()) {
      WebsocketControllerFactory servlet = new WebsocketControllerFactory(deviceAuthenticator,
                                                                          accountsManager,
                                                                          pushSender,
                                                                          storedMessages,
                                                                          pubSubManager);

      ServletRegistration.Dynamic websocket = environment.servlets().addServlet("WebSocket", servlet);
      websocket.addMapping("/v1/websocket/*");
      websocket.setAsyncSupported(true);

      FilterRegistration.Dynamic filter = environment.servlets().addFilter("CORS", CrossOriginFilter.class);
      filter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
      filter.setInitParameter("allowedOrigins", "*");
      filter.setInitParameter("allowedHeaders", "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin");
      filter.setInitParameter("allowedMethods", "GET,PUT,POST,DELETE,OPTIONS");
      filter.setInitParameter("preflightMaxAge", "5184000");
      filter.setInitParameter("allowCredentials", "true");
    }

    environment.healthChecks().register("redis", new RedisHealthCheck(redisClient));
    environment.healthChecks().register("memcache", new MemcacheHealthCheck(memcachedClient));

    environment.jersey().register(new IOExceptionMapper());
    environment.jersey().register(new RateLimitExceededExceptionMapper());

    environment.metrics().register(name(CpuUsageGauge.class, "cpu"), new CpuUsageGauge());
    environment.metrics().register(name(FreeMemoryGauge.class, "free_memory"), new FreeMemoryGauge());
    environment.metrics().register(name(NetworkSentGauge.class, "bytes_sent"), new NetworkSentGauge());
    environment.metrics().register(name(NetworkReceivedGauge.class, "bytes_received"), new NetworkReceivedGauge());

    if (config.getGraphiteConfiguration().isEnabled()) {
      GraphiteReporterFactory graphiteReporterFactory = new GraphiteReporterFactory();
      graphiteReporterFactory.setHost(config.getGraphiteConfiguration().getHost());
      graphiteReporterFactory.setPort(config.getGraphiteConfiguration().getPort());

      GraphiteReporter graphiteReporter = (GraphiteReporter) graphiteReporterFactory.build(environment.metrics());
      graphiteReporter.start(15, TimeUnit.SECONDS);
    }

    if (config.getMetricsConfiguration().isEnabled()) {
      new JsonMetricsReporter(environment.metrics(),
                              config.getMetricsConfiguration().getToken(),
                              config.getMetricsConfiguration().getHost())
          .start(60, TimeUnit.SECONDS);
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
