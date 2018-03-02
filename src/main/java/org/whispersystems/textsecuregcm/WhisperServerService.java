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
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.base.Optional;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.skife.jdbi.v2.DBI;
import org.whispersystems.dispatch.DispatchChannel;
import org.whispersystems.dispatch.DispatchManager;
import org.whispersystems.dropwizard.simpleauth.AuthDynamicFeature;
import org.whispersystems.dropwizard.simpleauth.AuthValueFactoryProvider;
import org.whispersystems.dropwizard.simpleauth.BasicCredentialAuthFilter;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.FederatedPeerAuthenticator;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AttachmentController;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.controllers.FederationControllerV1;
import org.whispersystems.textsecuregcm.controllers.FederationControllerV2;
import org.whispersystems.textsecuregcm.controllers.KeepAliveController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.ProvisioningController;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.liquibase.NameableMigrationsBundle;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.InvalidWebsocketAddressExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.CpuUsageGauge;
import org.whispersystems.textsecuregcm.metrics.FileDescriptorGauge;
import org.whispersystems.textsecuregcm.metrics.FreeMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.NetworkReceivedGauge;
import org.whispersystems.textsecuregcm.metrics.NetworkSentGauge;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.providers.RedisHealthCheck;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.push.WebsocketSender;
import org.whispersystems.textsecuregcm.s3.UrlSigner;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.Messages;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PendingAccounts;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingDevices;
import org.whispersystems.textsecuregcm.storage.PendingDevicesManager;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;
import org.whispersystems.textsecuregcm.websocket.DeadLetterHandler;
import org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.textsecuregcm.workers.DeleteUserCommand;
import org.whispersystems.textsecuregcm.workers.DirectoryCommand;
import org.whispersystems.textsecuregcm.workers.PeriodicStatsCommand;
import org.whispersystems.textsecuregcm.workers.TrimMessagesCommand;
import org.whispersystems.textsecuregcm.workers.VacuumCommand;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletRegistration;
import java.security.Security;
import java.util.EnumSet;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.Application;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.DBIFactory;
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
    bootstrap.addCommand(new VacuumCommand());
    bootstrap.addCommand(new TrimMessagesCommand());
    bootstrap.addCommand(new PeriodicStatsCommand());
    bootstrap.addCommand(new DeleteUserCommand());
    bootstrap.addBundle(new NameableMigrationsBundle<WhisperServerConfiguration>("accountdb", "accountsdb.xml") {
      @Override
      public DataSourceFactory getDataSourceFactory(WhisperServerConfiguration configuration) {
        return configuration.getDataSourceFactory();
      }
    });

    bootstrap.addBundle(new NameableMigrationsBundle<WhisperServerConfiguration>("messagedb", "messagedb.xml") {
      @Override
      public DataSourceFactory getDataSourceFactory(WhisperServerConfiguration configuration) {
        return configuration.getMessageStoreConfiguration();
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
    environment.getObjectMapper().setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    environment.getObjectMapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    DBIFactory dbiFactory = new DBIFactory();
    DBI        database   = dbiFactory.build(environment, config.getDataSourceFactory(), "accountdb");
    DBI        messagedb  = dbiFactory.build(environment, config.getMessageStoreConfiguration(), "messagedb");

    Accounts        accounts        = database.onDemand(Accounts.class);
    PendingAccounts pendingAccounts = database.onDemand(PendingAccounts.class);
    PendingDevices  pendingDevices  = database.onDemand(PendingDevices.class);
    Keys            keys            = database.onDemand(Keys.class);
    Messages        messages        = messagedb.onDemand(Messages.class);

    RedisClientFactory cacheClientFactory = new RedisClientFactory(config.getCacheConfiguration().getUrl());
    JedisPool          cacheClient        = cacheClientFactory.getRedisClientPool();
    JedisPool          directoryClient    = new RedisClientFactory(config.getDirectoryConfiguration().getUrl()).getRedisClientPool();

    DirectoryManager           directory                  = new DirectoryManager(directoryClient);
    PendingAccountsManager     pendingAccountsManager     = new PendingAccountsManager(pendingAccounts, cacheClient);
    PendingDevicesManager      pendingDevicesManager      = new PendingDevicesManager (pendingDevices, cacheClient );
    AccountsManager            accountsManager            = new AccountsManager(accounts, directory, cacheClient);
    FederatedClientManager     federatedClientManager     = new FederatedClientManager(environment, config.getJerseyClientConfiguration(), config.getFederationConfiguration());
    MessagesManager            messagesManager            = new MessagesManager(messages);
    DeadLetterHandler          deadLetterHandler          = new DeadLetterHandler(messagesManager);
    DispatchManager            dispatchManager            = new DispatchManager(cacheClientFactory, Optional.<DispatchChannel>of(deadLetterHandler));
    PubSubManager              pubSubManager              = new PubSubManager(cacheClient, dispatchManager);
    APNSender                  apnSender                  = new APNSender(accountsManager, config.getApnConfiguration());
    GCMSender                  gcmSender                  = new GCMSender(accountsManager, config.getGcmConfiguration().getApiKey());
    WebsocketSender            websocketSender            = new WebsocketSender(messagesManager, pubSubManager);
    AccountAuthenticator       deviceAuthenticator        = new AccountAuthenticator(accountsManager                 );
    FederatedPeerAuthenticator federatedPeerAuthenticator = new FederatedPeerAuthenticator(config.getFederationConfiguration());
    RateLimiters               rateLimiters               = new RateLimiters(config.getLimitsConfiguration(), cacheClient);

    ApnFallbackManager       apnFallbackManager  = new ApnFallbackManager(apnSender, pubSubManager);
    TwilioSmsSender          twilioSmsSender     = new TwilioSmsSender(config.getTwilioConfiguration());
    SmsSender                smsSender           = new SmsSender(twilioSmsSender);
    UrlSigner                urlSigner           = new UrlSigner(config.getAttachmentsConfiguration());
    PushSender               pushSender          = new PushSender(apnFallbackManager, gcmSender, apnSender, websocketSender, config.getPushConfiguration().getQueueSize());
    ReceiptSender            receiptSender       = new ReceiptSender(accountsManager, pushSender, federatedClientManager);
    TurnTokenGenerator       turnTokenGenerator  = new TurnTokenGenerator(config.getTurnConfiguration());

    apnSender.setApnFallbackManager(apnFallbackManager);
    environment.lifecycle().manage(apnFallbackManager);
    environment.lifecycle().manage(pubSubManager);
    environment.lifecycle().manage(pushSender);

    AttachmentController attachmentController = new AttachmentController(rateLimiters, federatedClientManager, urlSigner);
    KeysController       keysController       = new KeysController(rateLimiters, keys, accountsManager, federatedClientManager);
    MessageController    messageController    = new MessageController(rateLimiters, pushSender, receiptSender, accountsManager, messagesManager, federatedClientManager);
    ProfileController    profileController    = new ProfileController(rateLimiters , accountsManager, config.getProfilesConfiguration());

    environment.jersey().register(new AuthDynamicFeature(new BasicCredentialAuthFilter.Builder<Account>()
                                                             .setAuthenticator(deviceAuthenticator)
                                                             .setPrincipal(Account.class)
                                                             .buildAuthFilter(),
                                                         new BasicCredentialAuthFilter.Builder<FederatedPeer>()
                                                             .setAuthenticator(federatedPeerAuthenticator)
                                                             .setPrincipal(FederatedPeer.class)
                                                             .buildAuthFilter()));
    environment.jersey().register(new AuthValueFactoryProvider.Binder());

    environment.jersey().register(new AccountController(pendingAccountsManager, accountsManager, rateLimiters, smsSender, messagesManager, turnTokenGenerator, config.getTestDevices()));
    environment.jersey().register(new DeviceController(pendingDevicesManager, accountsManager, messagesManager, rateLimiters, config.getMaxDevices()));
    environment.jersey().register(new DirectoryController(rateLimiters, directory));
    environment.jersey().register(new FederationControllerV1(accountsManager, attachmentController, messageController));
    environment.jersey().register(new FederationControllerV2(accountsManager, attachmentController, messageController, keysController));
    environment.jersey().register(new ProvisioningController(rateLimiters, pushSender));
    environment.jersey().register(attachmentController);
    environment.jersey().register(keysController);
    environment.jersey().register(messageController);
    environment.jersey().register(profileController);

    ///
    WebSocketEnvironment webSocketEnvironment = new WebSocketEnvironment(environment, config.getWebSocketConfiguration(), 90000);
    webSocketEnvironment.setAuthenticator(new WebSocketAccountAuthenticator(deviceAuthenticator));
    webSocketEnvironment.setConnectListener(new AuthenticatedConnectListener(accountsManager, pushSender, receiptSender, messagesManager, pubSubManager));
    webSocketEnvironment.jersey().register(new KeepAliveController(pubSubManager));
    webSocketEnvironment.jersey().register(messageController);
    webSocketEnvironment.jersey().register(profileController);

    WebSocketEnvironment provisioningEnvironment = new WebSocketEnvironment(environment, webSocketEnvironment.getRequestLog(), 60000);
    provisioningEnvironment.setConnectListener(new ProvisioningConnectListener(pubSubManager));
    provisioningEnvironment.jersey().register(new KeepAliveController(pubSubManager));

    WebSocketResourceProviderFactory webSocketServlet    = new WebSocketResourceProviderFactory(webSocketEnvironment   );
    WebSocketResourceProviderFactory provisioningServlet = new WebSocketResourceProviderFactory(provisioningEnvironment);

    ServletRegistration.Dynamic websocket    = environment.servlets().addServlet("WebSocket", webSocketServlet      );
    ServletRegistration.Dynamic provisioning = environment.servlets().addServlet("Provisioning", provisioningServlet);

    websocket.addMapping("/v1/websocket/");
    websocket.setAsyncSupported(true);

    provisioning.addMapping("/v1/websocket/provisioning/");
    provisioning.setAsyncSupported(true);

    webSocketServlet.start();
    provisioningServlet.start();

    FilterRegistration.Dynamic filter = environment.servlets().addFilter("CORS", CrossOriginFilter.class);
    filter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    filter.setInitParameter("allowedOrigins", "*");
    filter.setInitParameter("allowedHeaders", "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin,X-Signal-Agent");
    filter.setInitParameter("allowedMethods", "GET,PUT,POST,DELETE,OPTIONS");
    filter.setInitParameter("preflightMaxAge", "5184000");
    filter.setInitParameter("allowCredentials", "true");
///

    environment.healthChecks().register("directory", new RedisHealthCheck(directoryClient));
    environment.healthChecks().register("cache", new RedisHealthCheck(cacheClient));

    environment.jersey().register(new IOExceptionMapper());
    environment.jersey().register(new RateLimitExceededExceptionMapper());
    environment.jersey().register(new InvalidWebsocketAddressExceptionMapper());
    environment.jersey().register(new DeviceLimitExceededExceptionMapper());

    environment.metrics().register(name(CpuUsageGauge.class, "cpu"), new CpuUsageGauge());
    environment.metrics().register(name(FreeMemoryGauge.class, "free_memory"), new FreeMemoryGauge());
    environment.metrics().register(name(NetworkSentGauge.class, "bytes_sent"), new NetworkSentGauge());
    environment.metrics().register(name(NetworkReceivedGauge.class, "bytes_received"), new NetworkReceivedGauge());
    environment.metrics().register(name(FileDescriptorGauge.class, "fd_count"), new FileDescriptorGauge());
  }

  public static void main(String[] args) throws Exception {
    new WhisperServerService().run(args);
  }
}
