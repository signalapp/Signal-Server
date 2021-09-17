/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm;

import static com.codahale.metrics.MetricRegistry.name;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jdbi3.strategies.DefaultNameStrategy;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.PolymorphicAuthDynamicFeature;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.datadog.DatadogMeterRegistry;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletRegistration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.jdbi.v3.core.Jdbi;
import org.signal.zkgroup.ServerSecretParams;
import org.signal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.zkgroup.profiles.ServerZkProfileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dispatch.DispatchManager;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.WebsocketRefreshApplicationEventListener;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.badges.ConfiguredProfileBadgeConverter;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.configuration.DirectoryServerConfiguration;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV1;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV2;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV3;
import org.whispersystems.textsecuregcm.controllers.CertificateController;
import org.whispersystems.textsecuregcm.controllers.ChallengeController;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.controllers.DirectoryController;
import org.whispersystems.textsecuregcm.controllers.DonationController;
import org.whispersystems.textsecuregcm.controllers.KeepAliveController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.PaymentsController;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.ProvisioningController;
import org.whispersystems.textsecuregcm.controllers.RemoteConfigController;
import org.whispersystems.textsecuregcm.controllers.SecureBackupController;
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.controllers.StickerController;
import org.whispersystems.textsecuregcm.controllers.VoiceVerificationController;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.currency.FixerClient;
import org.whispersystems.textsecuregcm.currency.FtxClient;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.filters.ContentLengthFilter;
import org.whispersystems.textsecuregcm.filters.RemoteDeprecationFilter;
import org.whispersystems.textsecuregcm.filters.TimestampResponseFilter;
import org.whispersystems.textsecuregcm.limits.PreKeyRateLimiter;
import org.whispersystems.textsecuregcm.limits.PushChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimitResetMetricsManager;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.limits.UnsealedSenderRateLimiter;
import org.whispersystems.textsecuregcm.liquibase.NameableMigrationsBundle;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.InvalidWebsocketAddressExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitChallengeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RetryLaterExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ServerRejectedExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.ApplicationShutdownMonitor;
import org.whispersystems.textsecuregcm.metrics.BufferPoolGauges;
import org.whispersystems.textsecuregcm.metrics.CpuUsageGauge;
import org.whispersystems.textsecuregcm.metrics.FileDescriptorGauge;
import org.whispersystems.textsecuregcm.metrics.FreeMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.GarbageCollectionGauges;
import org.whispersystems.textsecuregcm.metrics.MaxFileDescriptorGauge;
import org.whispersystems.textsecuregcm.metrics.MetricsApplicationEventListener;
import org.whispersystems.textsecuregcm.metrics.MetricsRequestEventListener;
import org.whispersystems.textsecuregcm.metrics.NetworkReceivedGauge;
import org.whispersystems.textsecuregcm.metrics.NetworkSentGauge;
import org.whispersystems.textsecuregcm.metrics.OperatingSystemMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.providers.RedisClientFactory;
import org.whispersystems.textsecuregcm.providers.RedisClusterHealthCheck;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.recaptcha.EnterpriseRecaptchaClient;
import org.whispersystems.textsecuregcm.recaptcha.LegacyRecaptchaClient;
import org.whispersystems.textsecuregcm.recaptcha.TransitionalRecaptchaClient;
import org.whispersystems.textsecuregcm.redis.ConnectionEventLogger;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioVerifyExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.AccountCleaner;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsDynamoDb;
import org.whispersystems.textsecuregcm.storage.AccountsDynamoDbMigrator;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ContactDiscoveryWriter;
import org.whispersystems.textsecuregcm.storage.DeletedAccounts;
import org.whispersystems.textsecuregcm.storage.DeletedAccountsDirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DeletedAccountsManager;
import org.whispersystems.textsecuregcm.storage.DeletedAccountsTableCrawler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciler;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagePersister;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.MigrationDeletedAccounts;
import org.whispersystems.textsecuregcm.storage.MigrationMismatchedAccounts;
import org.whispersystems.textsecuregcm.storage.MigrationMismatchedAccountsTableCrawler;
import org.whispersystems.textsecuregcm.storage.MigrationRetryAccounts;
import org.whispersystems.textsecuregcm.storage.MigrationRetryAccountsTableCrawler;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PushChallengeDynamoDb;
import org.whispersystems.textsecuregcm.storage.PushFeedbackProcessor;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageDynamoDb;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.storage.VerificationCodeStore;
import org.whispersystems.textsecuregcm.util.AsnManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import org.whispersystems.textsecuregcm.util.HostnameUtil;
import org.whispersystems.textsecuregcm.util.TorExitNodeManager;
import org.whispersystems.textsecuregcm.util.logging.LoggingUnhandledExceptionMapper;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;
import org.whispersystems.textsecuregcm.websocket.DeadLetterHandler;
import org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.textsecuregcm.workers.CertificateCommand;
import org.whispersystems.textsecuregcm.workers.CheckDynamicConfigurationCommand;
import org.whispersystems.textsecuregcm.workers.DeleteUserCommand;
import org.whispersystems.textsecuregcm.workers.ServerVersionCommand;
import org.whispersystems.textsecuregcm.workers.SetCrawlerAccelerationTask;
import org.whispersystems.textsecuregcm.workers.SetRequestLoggingEnabledTask;
import org.whispersystems.textsecuregcm.workers.SetUserDiscoverabilityCommand;
import org.whispersystems.textsecuregcm.workers.VacuumCommand;
import org.whispersystems.textsecuregcm.workers.ZkParamsCommand;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.setup.WebSocketEnvironment;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

public class WhisperServerService extends Application<WhisperServerConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(WhisperServerService.class);

  @Override
  public void initialize(Bootstrap<WhisperServerConfiguration> bootstrap) {
    bootstrap.addCommand(new VacuumCommand());
    bootstrap.addCommand(new DeleteUserCommand());
    bootstrap.addCommand(new CertificateCommand());
    bootstrap.addCommand(new ZkParamsCommand());
    bootstrap.addCommand(new ServerVersionCommand());
    bootstrap.addCommand(new CheckDynamicConfigurationCommand());
    bootstrap.addCommand(new SetUserDiscoverabilityCommand());

    bootstrap.addBundle(new NameableMigrationsBundle<WhisperServerConfiguration>("accountdb", "accountsdb.xml") {
      @Override
      public DataSourceFactory getDataSourceFactory(WhisperServerConfiguration configuration) {
        return configuration.getAccountsDatabaseConfiguration();
      }
    });


    bootstrap.addBundle(new NameableMigrationsBundle<WhisperServerConfiguration>("abusedb", "abusedb.xml") {
      @Override
      public PooledDataSourceFactory getDataSourceFactory(WhisperServerConfiguration configuration) {
        return configuration.getAbuseDatabaseConfiguration();
      }
    });
  }

  @Override
  public String getName() {
    return "whisper-server";
  }

  @Override
  public void run(WhisperServerConfiguration config, Environment environment)
      throws Exception {

    UncaughtExceptionHandler.register();

    SharedMetricRegistries.add(Constants.METRICS_NAME, environment.metrics());

    final DistributionStatisticConfig defaultDistributionStatisticConfig = DistributionStatisticConfig.builder()
        .percentiles(.75, .95, .99, .999)
        .build();

    {
      final DatadogMeterRegistry datadogMeterRegistry = new DatadogMeterRegistry(
          config.getDatadogConfiguration(), io.micrometer.core.instrument.Clock.SYSTEM);

      datadogMeterRegistry.config().commonTags(
          Tags.of(
              "service", "chat",
              "host", HostnameUtil.getLocalHostname(),
              "version", WhisperServerVersion.getServerVersion(),
              "env", config.getDatadogConfiguration().getEnvironment()))
          .meterFilter(MeterFilter.denyNameStartsWith(MetricsRequestEventListener.REQUEST_COUNTER_NAME))
          .meterFilter(MeterFilter.denyNameStartsWith(MetricsRequestEventListener.ANDROID_REQUEST_COUNTER_NAME))
          .meterFilter(MeterFilter.denyNameStartsWith(MetricsRequestEventListener.DESKTOP_REQUEST_COUNTER_NAME))
          .meterFilter(MeterFilter.denyNameStartsWith(MetricsRequestEventListener.IOS_REQUEST_COUNTER_NAME))
          .meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(final Id id, final DistributionStatisticConfig config) {
              return defaultDistributionStatisticConfig.merge(config);
            }
          });

      Metrics.addRegistry(datadogMeterRegistry);
    }

    environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    environment.getObjectMapper().setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    environment.getObjectMapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    ProfileBadgeConverter profileBadgeConverter = new ConfiguredProfileBadgeConverter(
        Clock.systemUTC(), config.getBadges());

    JdbiFactory jdbiFactory = new JdbiFactory(DefaultNameStrategy.CHECK_EMPTY);
    Jdbi        accountJdbi = jdbiFactory.build(environment, config.getAccountsDatabaseConfiguration(), "accountdb");
    Jdbi        abuseJdbi   = jdbiFactory.build(environment, config.getAbuseDatabaseConfiguration(), "abusedb"  );

    FaultTolerantDatabase accountDatabase = new FaultTolerantDatabase("accounts_database", accountJdbi, config.getAccountsDatabaseConfiguration().getCircuitBreakerConfiguration());
    FaultTolerantDatabase abuseDatabase   = new FaultTolerantDatabase("abuse_database", abuseJdbi, config.getAbuseDatabaseConfiguration().getCircuitBreakerConfiguration());

    DynamoDbClient messageDynamoDb = DynamoDbFromConfig.client(config.getMessageDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient preKeyDynamoDb = DynamoDbFromConfig.client(config.getKeysDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient accountsDynamoDbClient = DynamoDbFromConfig.client(config.getAccountsDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    // The thread pool core & max sizes are set via dynamic configuration within AccountsDynamoDb
    ThreadPoolExecutor accountsDynamoDbMigrationThreadPool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
        new LinkedBlockingDeque<>());

    DynamoDbAsyncClient accountsDynamoDbAsyncClient = DynamoDbFromConfig.asyncClient(config.getAccountsDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create(),
        accountsDynamoDbMigrationThreadPool);

    DynamoDbClient deletedAccountsDynamoDbClient = DynamoDbFromConfig.client(config.getDeletedAccountsDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient recentlyDeletedAccountsDynamoDb = DynamoDbFromConfig.client(config.getMigrationDeletedAccountsDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient pushChallengeDynamoDbClient = DynamoDbFromConfig.client(
        config.getPushChallengeDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient reportMessageDynamoDbClient = DynamoDbFromConfig.client(
        config.getReportMessageDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient migrationRetryAccountsDynamoDb = DynamoDbFromConfig.client(
        config.getMigrationRetryAccountsDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient migrationMismatchedAccountsDynamoDb = DynamoDbFromConfig.client(
        config.getMigrationMismatchedAccountsDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient pendingAccountsDynamoDbClient = DynamoDbFromConfig.client(
        config.getPendingAccountsDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    DynamoDbClient pendingDevicesDynamoDbClient = DynamoDbFromConfig.client(
        config.getPendingDevicesDynamoDbConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    AmazonDynamoDB deletedAccountsLockDynamoDbClient = AmazonDynamoDBClientBuilder.standard()
        .withRegion(config.getDeletedAccountsLockDynamoDbConfiguration().getRegion())
        .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(
                ((int) config.getDeletedAccountsLockDynamoDbConfiguration().getClientExecutionTimeout().toMillis()))
            .withRequestTimeout(
                (int) config.getDeletedAccountsLockDynamoDbConfiguration().getClientRequestTimeout().toMillis()))
        .withCredentials(InstanceProfileCredentialsProvider.getInstance())
        .build();

    DeletedAccounts deletedAccounts = new DeletedAccounts(deletedAccountsDynamoDbClient,
        config.getDeletedAccountsDynamoDbConfiguration().getTableName(),
        config.getDeletedAccountsDynamoDbConfiguration().getNeedsReconciliationIndexName());
    MigrationDeletedAccounts migrationDeletedAccounts = new MigrationDeletedAccounts(recentlyDeletedAccountsDynamoDb,
        config.getMigrationDeletedAccountsDynamoDbConfiguration().getTableName());
    MigrationRetryAccounts migrationRetryAccounts = new MigrationRetryAccounts(migrationRetryAccountsDynamoDb,
        config.getMigrationRetryAccountsDynamoDbConfiguration().getTableName());
    MigrationMismatchedAccounts mismatchedAccounts = new MigrationMismatchedAccounts(
        migrationMismatchedAccountsDynamoDb,
        config.getMigrationMismatchedAccountsDynamoDbConfiguration().getTableName());

    Accounts accounts = new Accounts(accountDatabase);
    AccountsDynamoDb accountsDynamoDb = new AccountsDynamoDb(accountsDynamoDbClient, accountsDynamoDbAsyncClient,
        accountsDynamoDbMigrationThreadPool, config.getAccountsDynamoDbConfiguration().getTableName(),
        config.getAccountsDynamoDbConfiguration().getPhoneNumberTableName(), migrationDeletedAccounts,
        migrationRetryAccounts);
    Usernames usernames = new Usernames(accountDatabase);
    ReservedUsernames reservedUsernames = new ReservedUsernames(accountDatabase);
    Profiles profiles = new Profiles(accountDatabase);
    KeysDynamoDb keysDynamoDb = new KeysDynamoDb(preKeyDynamoDb, config.getKeysDynamoDbConfiguration().getTableName());
    MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(messageDynamoDb,
        config.getMessageDynamoDbConfiguration().getTableName(),
        config.getMessageDynamoDbConfiguration().getTimeToLive());
    AbusiveHostRules abusiveHostRules = new AbusiveHostRules(abuseDatabase);
    RemoteConfigs remoteConfigs = new RemoteConfigs(accountDatabase);
    PushChallengeDynamoDb pushChallengeDynamoDb = new PushChallengeDynamoDb(pushChallengeDynamoDbClient, config.getPushChallengeDynamoDbConfiguration().getTableName());
    ReportMessageDynamoDb reportMessageDynamoDb = new ReportMessageDynamoDb(reportMessageDynamoDbClient, config.getReportMessageDynamoDbConfiguration().getTableName());
    VerificationCodeStore pendingAccounts = new VerificationCodeStore(pendingAccountsDynamoDbClient, config.getPendingAccountsDynamoDbConfiguration().getTableName());
    VerificationCodeStore pendingDevices = new VerificationCodeStore(pendingDevicesDynamoDbClient, config.getPendingDevicesDynamoDbConfiguration().getTableName());

    RedisClientFactory  pubSubClientFactory = new RedisClientFactory("pubsub_cache", config.getPubsubCacheConfiguration().getUrl(), config.getPubsubCacheConfiguration().getReplicaUrls(), config.getPubsubCacheConfiguration().getCircuitBreakerConfiguration());
    ReplicatedJedisPool pubsubClient        = pubSubClientFactory.getRedisClientPool();

    ClientResources generalCacheClientResources       = ClientResources.builder().build();
    ClientResources messageCacheClientResources       = ClientResources.builder().build();
    ClientResources presenceClientResources           = ClientResources.builder().build();
    ClientResources metricsCacheClientResources       = ClientResources.builder().build();
    ClientResources pushSchedulerCacheClientResources = ClientResources.builder().ioThreadPoolSize(4).build();
    ClientResources rateLimitersCacheClientResources =  ClientResources.builder().build();

    ConnectionEventLogger.logConnectionEvents(generalCacheClientResources);
    ConnectionEventLogger.logConnectionEvents(messageCacheClientResources);
    ConnectionEventLogger.logConnectionEvents(presenceClientResources);
    ConnectionEventLogger.logConnectionEvents(metricsCacheClientResources);

    FaultTolerantRedisCluster cacheCluster             = new FaultTolerantRedisCluster("main_cache_cluster", config.getCacheClusterConfiguration(), generalCacheClientResources);
    FaultTolerantRedisCluster messagesCluster          = new FaultTolerantRedisCluster("messages_cluster", config.getMessageCacheConfiguration().getRedisClusterConfiguration(), messageCacheClientResources);
    FaultTolerantRedisCluster clientPresenceCluster    = new FaultTolerantRedisCluster("client_presence_cluster", config.getClientPresenceClusterConfiguration(), presenceClientResources);
    FaultTolerantRedisCluster metricsCluster           = new FaultTolerantRedisCluster("metrics_cluster", config.getMetricsClusterConfiguration(), metricsCacheClientResources);
    FaultTolerantRedisCluster pushSchedulerCluster     = new FaultTolerantRedisCluster("push_scheduler", config.getPushSchedulerCluster(), pushSchedulerCacheClientResources);
    FaultTolerantRedisCluster rateLimitersCluster      = new FaultTolerantRedisCluster("rate_limiters", config.getRateLimitersCluster(), rateLimitersCacheClientResources);

    BlockingQueue<Runnable> keyspaceNotificationDispatchQueue = new ArrayBlockingQueue<>(10_000);
    Metrics.gaugeCollectionSize(name(getClass(), "keyspaceNotificationDispatchQueueSize"), Collections.emptyList(), keyspaceNotificationDispatchQueue);

    ScheduledExecutorService recurringJobExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "recurringJob-%d")).threads(6).build();
    ScheduledExecutorService declinedMessageReceiptExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "declined-receipt-%d")).threads(2).build();
    ScheduledExecutorService retrySchedulingExecutor              = environment.lifecycle().scheduledExecutorService(name(getClass(), "retry-%d")).threads(2).build();
    ExecutorService          keyspaceNotificationDispatchExecutor = environment.lifecycle().executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(16).workQueue(keyspaceNotificationDispatchQueue).build();
    ExecutorService          apnSenderExecutor                    = environment.lifecycle().executorService(name(getClass(), "apnSender-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService          gcmSenderExecutor                    = environment.lifecycle().executorService(name(getClass(), "gcmSender-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService          backupServiceExecutor                = environment.lifecycle().executorService(name(getClass(), "backupService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService          storageServiceExecutor               = environment.lifecycle().executorService(name(getClass(), "storageService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService          donationExecutor                     = environment.lifecycle().executorService(name(getClass(), "donation-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService multiRecipientMessageExecutor = environment.lifecycle()
        .executorService(name(getClass(), "multiRecipientMessage-%d")).minThreads(64).maxThreads(64).build();
    ExecutorService accountsCrawlerChunkPreReadExecutor = environment.lifecycle()
        .executorService(name(getClass(), "accountsCrawler-%d")).maxThreads(2).minThreads(2).build();

    ExternalServiceCredentialGenerator directoryCredentialsGenerator = new ExternalServiceCredentialGenerator(config.getDirectoryConfiguration().getDirectoryClientConfiguration().getUserAuthenticationTokenSharedSecret(),
            config.getDirectoryConfiguration().getDirectoryClientConfiguration().getUserAuthenticationTokenUserIdSecret(),
            true);

    DynamicConfigurationManager dynamicConfigurationManager = new DynamicConfigurationManager(config.getAppConfig().getApplication(), config.getAppConfig().getEnvironment(), config.getAppConfig().getConfigurationName());
    dynamicConfigurationManager.start();

    ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    TwilioVerifyExperimentEnrollmentManager verifyExperimentEnrollmentManager = new TwilioVerifyExperimentEnrollmentManager(
        config.getVoiceVerificationConfiguration(), experimentEnrollmentManager);

    ExternalServiceCredentialGenerator storageCredentialsGenerator   = new ExternalServiceCredentialGenerator(config.getSecureStorageServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);
    ExternalServiceCredentialGenerator backupCredentialsGenerator    = new ExternalServiceCredentialGenerator(config.getSecureBackupServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);
    ExternalServiceCredentialGenerator paymentsCredentialsGenerator  = new ExternalServiceCredentialGenerator(config.getPaymentsServiceConfiguration().getUserAuthenticationTokenSharedSecret(), new byte[0], false);

    SecureBackupClient         secureBackupClient         = new SecureBackupClient(backupCredentialsGenerator, backupServiceExecutor, config.getSecureBackupServiceConfiguration());
    SecureStorageClient        secureStorageClient        = new SecureStorageClient(storageCredentialsGenerator, storageServiceExecutor, config.getSecureStorageServiceConfiguration());
    ClientPresenceManager      clientPresenceManager      = new ClientPresenceManager(clientPresenceCluster, recurringJobExecutor, keyspaceNotificationDispatchExecutor);
    DirectoryQueue             directoryQueue             = new DirectoryQueue(config.getDirectoryConfiguration().getSqsConfiguration());
    StoredVerificationCodeManager pendingAccountsManager  = new StoredVerificationCodeManager(pendingAccounts);
    StoredVerificationCodeManager pendingDevicesManager   = new StoredVerificationCodeManager(pendingDevices);
    UsernamesManager           usernamesManager           = new UsernamesManager(usernames, reservedUsernames, cacheCluster);
    ProfilesManager            profilesManager            = new ProfilesManager(profiles, cacheCluster);
    MessagesCache              messagesCache              = new MessagesCache(messagesCluster, messagesCluster, keyspaceNotificationDispatchExecutor);
    PushLatencyManager         pushLatencyManager         = new PushLatencyManager(metricsCluster);
    ReportMessageManager       reportMessageManager       = new ReportMessageManager(reportMessageDynamoDb, Metrics.globalRegistry);
    MessagesManager            messagesManager            = new MessagesManager(messagesDynamoDb, messagesCache, pushLatencyManager, reportMessageManager);
    DeletedAccountsManager deletedAccountsManager = new DeletedAccountsManager(deletedAccounts,
        deletedAccountsLockDynamoDbClient, config.getDeletedAccountsLockDynamoDbConfiguration().getTableName());
    AccountsManager accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster,
        deletedAccountsManager, directoryQueue, keysDynamoDb, messagesManager, mismatchedAccounts, usernamesManager,
        profilesManager, pendingAccountsManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager,
        dynamicConfigurationManager);
    RemoteConfigsManager remoteConfigsManager = new RemoteConfigsManager(remoteConfigs);
    DeadLetterHandler          deadLetterHandler          = new DeadLetterHandler(accountsManager, messagesManager);
    DispatchManager            dispatchManager            = new DispatchManager(pubSubClientFactory, Optional.of(deadLetterHandler));
    PubSubManager              pubSubManager              = new PubSubManager(pubsubClient, dispatchManager);
    APNSender                  apnSender                  = new APNSender(apnSenderExecutor, accountsManager, config.getApnConfiguration());
    GCMSender                  gcmSender                  = new GCMSender(gcmSenderExecutor, accountsManager, config.getGcmConfiguration().getApiKey());
    RateLimiters               rateLimiters               = new RateLimiters(config.getLimitsConfiguration(), dynamicConfigurationManager, rateLimitersCluster);
    ProvisioningManager        provisioningManager        = new ProvisioningManager(pubSubManager);
    TorExitNodeManager         torExitNodeManager         = new TorExitNodeManager(recurringJobExecutor, config.getTorExitNodeListConfiguration());
    AsnManager                 asnManager                 = new AsnManager(recurringJobExecutor, config.getAsnTableConfiguration());

    AccountAuthenticator                  accountAuthenticator                  = new AccountAuthenticator(accountsManager);
    DisabledPermittedAccountAuthenticator disabledPermittedAccountAuthenticator = new DisabledPermittedAccountAuthenticator(accountsManager);

    RateLimitResetMetricsManager rateLimitResetMetricsManager = new RateLimitResetMetricsManager(metricsCluster, Metrics.globalRegistry);

    UnsealedSenderRateLimiter unsealedSenderRateLimiter = new UnsealedSenderRateLimiter(rateLimiters, rateLimitersCluster, dynamicConfigurationManager, rateLimitResetMetricsManager);
    PreKeyRateLimiter preKeyRateLimiter = new PreKeyRateLimiter(rateLimiters, dynamicConfigurationManager, rateLimitResetMetricsManager);

    ApnFallbackManager       apnFallbackManager = new ApnFallbackManager(pushSchedulerCluster, apnSender, accountsManager);
    TwilioSmsSender          twilioSmsSender    = new TwilioSmsSender(config.getTwilioConfiguration(), dynamicConfigurationManager);
    SmsSender                smsSender          = new SmsSender(twilioSmsSender);
    MessageSender            messageSender      = new MessageSender(apnFallbackManager, clientPresenceManager, messagesManager, gcmSender, apnSender, pushLatencyManager);
    ReceiptSender            receiptSender      = new ReceiptSender(accountsManager, messageSender);
    TurnTokenGenerator       turnTokenGenerator = new TurnTokenGenerator(config.getTurnConfiguration());
    LegacyRecaptchaClient legacyRecaptchaClient = new LegacyRecaptchaClient(config.getRecaptchaConfiguration().getSecret());
    EnterpriseRecaptchaClient enterpriseRecaptchaClient = new EnterpriseRecaptchaClient(
        config.getRecaptchaV2Configuration().getScoreFloor().doubleValue(),
        config.getRecaptchaV2Configuration().getSiteKey(),
        config.getRecaptchaV2Configuration().getProjectPath(),
        config.getRecaptchaV2Configuration().getCredentialConfigurationJson());
    TransitionalRecaptchaClient transitionalRecaptchaClient = new TransitionalRecaptchaClient(legacyRecaptchaClient, enterpriseRecaptchaClient);
    PushChallengeManager     pushChallengeManager = new PushChallengeManager(apnSender, gcmSender, pushChallengeDynamoDb);
    RateLimitChallengeManager rateLimitChallengeManager = new RateLimitChallengeManager(pushChallengeManager,
        transitionalRecaptchaClient, preKeyRateLimiter, unsealedSenderRateLimiter, rateLimiters,
        dynamicConfigurationManager);

    MessagePersister messagePersister = new MessagePersister(messagesCache, messagesManager, accountsManager, dynamicConfigurationManager, Duration.ofMinutes(config.getMessageCacheConfiguration().getPersistDelayMinutes()));

    // TODO listeners must be ordered so that ones that directly update accounts come last, so that read-only ones are not working with stale data
    final List<AccountDatabaseCrawlerListener> accountDatabaseCrawlerListeners = new ArrayList<>();

    final List<DeletedAccountsDirectoryReconciler> deletedAccountsDirectoryReconcilers = new ArrayList<>();
    for (DirectoryServerConfiguration directoryServerConfiguration : config.getDirectoryConfiguration()
        .getDirectoryServerConfiguration()) {
      final DirectoryReconciliationClient directoryReconciliationClient = new DirectoryReconciliationClient(
          directoryServerConfiguration);
      final DirectoryReconciler directoryReconciler = new DirectoryReconciler(
          directoryServerConfiguration.getReplicationName(), directoryReconciliationClient);
      // reconcilers are read-only
      accountDatabaseCrawlerListeners.add(directoryReconciler);

      final DeletedAccountsDirectoryReconciler deletedAccountsDirectoryReconciler = new DeletedAccountsDirectoryReconciler(
          directoryServerConfiguration.getReplicationName(), directoryReconciliationClient);
      deletedAccountsDirectoryReconcilers.add(deletedAccountsDirectoryReconciler);
    }
    accountDatabaseCrawlerListeners.add(new ContactDiscoveryWriter(accountsManager));
    // PushFeedbackProcessor may update device properties
    accountDatabaseCrawlerListeners.add(new PushFeedbackProcessor(accountsManager));
    // delete accounts last
    accountDatabaseCrawlerListeners.add(new AccountCleaner(accountsManager));

    HttpClient                currencyClient  = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(10)).build();
    FixerClient               fixerClient     = new FixerClient(currencyClient, config.getPaymentsServiceConfiguration().getFixerApiKey());
    FtxClient                 ftxClient       = new FtxClient(currencyClient);
    CurrencyConversionManager currencyManager = new CurrencyConversionManager(fixerClient, ftxClient, config.getPaymentsServiceConfiguration().getPaymentCurrencies());

    AccountDatabaseCrawlerCache accountDatabaseCrawlerCache = new AccountDatabaseCrawlerCache(cacheCluster);
    AccountDatabaseCrawler accountDatabaseCrawler = new AccountDatabaseCrawler(accountsManager,
        accountDatabaseCrawlerCache, accountDatabaseCrawlerListeners,
        config.getAccountDatabaseCrawlerConfiguration().getChunkSize(),
        config.getAccountDatabaseCrawlerConfiguration().getChunkIntervalMs(),
        accountsCrawlerChunkPreReadExecutor,
        dynamicConfigurationManager);

    AccountDatabaseCrawlerCache dynamoDbMigrationCrawlerCache = new AccountDatabaseCrawlerCache(cacheCluster);
    dynamoDbMigrationCrawlerCache.setPrefix("DynamoMigration");
    AccountDatabaseCrawler accountDynamoDbMigrationCrawler = new AccountDatabaseCrawler(accountsManager,
        dynamoDbMigrationCrawlerCache,
        List.of(new AccountsDynamoDbMigrator(accountsDynamoDb, dynamicConfigurationManager)),
        config.getDynamoDbMigrationCrawlerConfiguration().getChunkSize(),
        config.getDynamoDbMigrationCrawlerConfiguration().getChunkIntervalMs(),
        accountsCrawlerChunkPreReadExecutor,
        dynamicConfigurationManager);
    accountDynamoDbMigrationCrawler.setDedicatedDynamoMigrationCrawler(true);

    DeletedAccountsTableCrawler deletedAccountsTableCrawler = new DeletedAccountsTableCrawler(deletedAccountsManager, deletedAccountsDirectoryReconcilers, cacheCluster, recurringJobExecutor);
    MigrationRetryAccountsTableCrawler migrationRetryAccountsTableCrawler = new MigrationRetryAccountsTableCrawler(
        migrationRetryAccounts, accountsManager, accountsDynamoDb, cacheCluster, recurringJobExecutor);
    MigrationMismatchedAccountsTableCrawler migrationMismatchedAccountsTableCrawler = new MigrationMismatchedAccountsTableCrawler(
        mismatchedAccounts, accountsManager, accounts, accountsDynamoDb, dynamicConfigurationManager, cacheCluster,
        recurringJobExecutor);

    apnSender.setApnFallbackManager(apnFallbackManager);
    environment.lifecycle().manage(new ApplicationShutdownMonitor());
    environment.lifecycle().manage(apnFallbackManager);
    environment.lifecycle().manage(pubSubManager);
    environment.lifecycle().manage(messageSender);
    environment.lifecycle().manage(accountDatabaseCrawler);
    environment.lifecycle().manage(accountDynamoDbMigrationCrawler);
    environment.lifecycle().manage(deletedAccountsTableCrawler);
    environment.lifecycle().manage(migrationRetryAccountsTableCrawler);
    environment.lifecycle().manage(migrationMismatchedAccountsTableCrawler);
    environment.lifecycle().manage(remoteConfigsManager);
    environment.lifecycle().manage(messagesCache);
    environment.lifecycle().manage(messagePersister);
    environment.lifecycle().manage(clientPresenceManager);
    environment.lifecycle().manage(currencyManager);
    environment.lifecycle().manage(torExitNodeManager);
    environment.lifecycle().manage(asnManager);
    environment.lifecycle().manage(directoryQueue);

    StaticCredentialsProvider cdnCredentialsProvider = StaticCredentialsProvider
        .create(AwsBasicCredentials.create(
            config.getCdnConfiguration().getAccessKey(),
            config.getCdnConfiguration().getAccessSecret()));
    S3Client cdnS3Client               = S3Client.builder()
        .credentialsProvider(cdnCredentialsProvider)
        .region(Region.of(config.getCdnConfiguration().getRegion()))
        .build();
    PostPolicyGenerator profileCdnPolicyGenerator = new PostPolicyGenerator(config.getCdnConfiguration().getRegion(),
        config.getCdnConfiguration().getBucket(), config.getCdnConfiguration().getAccessKey());
    PolicySigner profileCdnPolicySigner = new PolicySigner(config.getCdnConfiguration().getAccessSecret(),
        config.getCdnConfiguration().getRegion());

    ServerSecretParams zkSecretParams = new ServerSecretParams(config.getZkConfig().getServerSecret());
    ServerZkProfileOperations zkProfileOperations = new ServerZkProfileOperations(zkSecretParams);
    ServerZkAuthOperations zkAuthOperations = new ServerZkAuthOperations(zkSecretParams);
    boolean isZkEnabled = config.getZkConfig().isEnabled();

    AuthFilter<BasicCredentials, AuthenticatedAccount> accountAuthFilter = new BasicCredentialAuthFilter.Builder<AuthenticatedAccount>().setAuthenticator(
        accountAuthenticator).buildAuthFilter();
    AuthFilter<BasicCredentials, DisabledPermittedAuthenticatedAccount> disabledPermittedAccountAuthFilter = new BasicCredentialAuthFilter.Builder<DisabledPermittedAuthenticatedAccount>().setAuthenticator(
        disabledPermittedAccountAuthenticator).buildAuthFilter();

    environment.servlets()
        .addFilter("RemoteDeprecationFilter", new RemoteDeprecationFilter(dynamicConfigurationManager))
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");

    environment.jersey().register(new ContentLengthFilter(TrafficSource.HTTP));
    environment.jersey().register(MultiRecipientMessageProvider.class);
    environment.jersey().register(new MetricsApplicationEventListener(TrafficSource.HTTP));
    environment.jersey()
        .register(new PolymorphicAuthDynamicFeature<>(ImmutableMap.of(AuthenticatedAccount.class, accountAuthFilter,
            DisabledPermittedAuthenticatedAccount.class, disabledPermittedAccountAuthFilter)));
    environment.jersey().register(new PolymorphicAuthValueFactoryProvider.Binder<>(
        ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)));
    environment.jersey().register(new WebsocketRefreshApplicationEventListener(clientPresenceManager));
    environment.jersey().register(new TimestampResponseFilter());
    environment.jersey().register(new VoiceVerificationController(config.getVoiceVerificationConfiguration().getUrl(),
        config.getVoiceVerificationConfiguration().getLocales()));

    ///
    WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment = new WebSocketEnvironment<>(environment,
        config.getWebSocketConfiguration(), 90000);
    webSocketEnvironment.setAuthenticator(new WebSocketAccountAuthenticator(accountAuthenticator));
    webSocketEnvironment.setConnectListener(
        new AuthenticatedConnectListener(receiptSender, messagesManager, messageSender, apnFallbackManager,
            clientPresenceManager, retrySchedulingExecutor));
    webSocketEnvironment.jersey().register(new WebsocketRefreshApplicationEventListener(clientPresenceManager));
    webSocketEnvironment.jersey().register(new ContentLengthFilter(TrafficSource.WEBSOCKET));
    webSocketEnvironment.jersey().register(MultiRecipientMessageProvider.class);
    webSocketEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET));
    webSocketEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));

    // these should be common, but use @Auth DisabledPermittedAccount, which isn’t supported yet on websocket
    environment.jersey().register(
        new AccountController(pendingAccountsManager, accountsManager, usernamesManager, abusiveHostRules, rateLimiters,
            smsSender, dynamicConfigurationManager, turnTokenGenerator, config.getTestDevices(),
            transitionalRecaptchaClient, gcmSender, apnSender, backupCredentialsGenerator,
            verifyExperimentEnrollmentManager));
    environment.jersey().register(new KeysController(rateLimiters, keysDynamoDb, accountsManager, preKeyRateLimiter, rateLimitChallengeManager));

    final List<Object> commonControllers = List.of(
        new AttachmentControllerV1(rateLimiters, config.getAwsAttachmentsConfiguration().getAccessKey(), config.getAwsAttachmentsConfiguration().getAccessSecret(), config.getAwsAttachmentsConfiguration().getBucket()),
        new AttachmentControllerV2(rateLimiters, config.getAwsAttachmentsConfiguration().getAccessKey(), config.getAwsAttachmentsConfiguration().getAccessSecret(), config.getAwsAttachmentsConfiguration().getRegion(), config.getAwsAttachmentsConfiguration().getBucket()),
        new AttachmentControllerV3(rateLimiters, config.getGcpAttachmentsConfiguration().getDomain(), config.getGcpAttachmentsConfiguration().getEmail(), config.getGcpAttachmentsConfiguration().getMaxSizeInBytes(), config.getGcpAttachmentsConfiguration().getPathPrefix(), config.getGcpAttachmentsConfiguration().getRsaSigningKey()),
        new CertificateController(new CertificateGenerator(config.getDeliveryCertificate().getCertificate(), config.getDeliveryCertificate().getPrivateKey(), config.getDeliveryCertificate().getExpiresDays()), zkAuthOperations, isZkEnabled),
        new ChallengeController(rateLimitChallengeManager),
        new DeviceController(pendingDevicesManager, accountsManager, messagesManager, keysDynamoDb, rateLimiters, config.getMaxDevices()),
        new DirectoryController(directoryCredentialsGenerator),
        new DonationController(donationExecutor, config.getDonationConfiguration()),
        new MessageController(rateLimiters, messageSender, receiptSender, accountsManager, messagesManager, unsealedSenderRateLimiter, apnFallbackManager, dynamicConfigurationManager, rateLimitChallengeManager, reportMessageManager, metricsCluster, declinedMessageReceiptExecutor, multiRecipientMessageExecutor),
        new PaymentsController(currencyManager, paymentsCredentialsGenerator),
        new ProfileController(rateLimiters, accountsManager, profilesManager, usernamesManager, dynamicConfigurationManager, profileBadgeConverter, cdnS3Client, profileCdnPolicyGenerator, profileCdnPolicySigner, config.getCdnConfiguration().getBucket(), zkProfileOperations, isZkEnabled),
        new ProvisioningController(rateLimiters, provisioningManager),
        new RemoteConfigController(remoteConfigsManager, config.getRemoteConfigConfiguration().getAuthorizedTokens(), config.getRemoteConfigConfiguration().getGlobalConfig()),
        new SecureBackupController(backupCredentialsGenerator),
        new SecureStorageController(storageCredentialsGenerator),
        new StickerController(rateLimiters, config.getCdnConfiguration().getAccessKey(),
            config.getCdnConfiguration().getAccessSecret(), config.getCdnConfiguration().getRegion(),
            config.getCdnConfiguration().getBucket())
    );

    for (Object controller : commonControllers) {
      environment.jersey().register(controller);
      webSocketEnvironment.jersey().register(controller);
    }

    WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment = new WebSocketEnvironment<>(environment,
        webSocketEnvironment.getRequestLog(), 60000);
    provisioningEnvironment.jersey().register(new WebsocketRefreshApplicationEventListener(clientPresenceManager));
    provisioningEnvironment.setConnectListener(new ProvisioningConnectListener(pubSubManager));
    provisioningEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET));
    provisioningEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));

    registerCorsFilter(environment);
    registerExceptionMappers(environment, webSocketEnvironment, provisioningEnvironment);

    RateLimitChallengeExceptionMapper rateLimitChallengeExceptionMapper = new RateLimitChallengeExceptionMapper(
        rateLimitChallengeManager);

    environment.jersey().register(rateLimitChallengeExceptionMapper);
    webSocketEnvironment.jersey().register(rateLimitChallengeExceptionMapper);
    provisioningEnvironment.jersey().register(rateLimitChallengeExceptionMapper);

    WebSocketResourceProviderFactory<AuthenticatedAccount> webSocketServlet = new WebSocketResourceProviderFactory<>(
        webSocketEnvironment, AuthenticatedAccount.class, config.getWebSocketConfiguration());
    WebSocketResourceProviderFactory<AuthenticatedAccount> provisioningServlet = new WebSocketResourceProviderFactory<>(
        provisioningEnvironment, AuthenticatedAccount.class, config.getWebSocketConfiguration());

    ServletRegistration.Dynamic websocket = environment.servlets().addServlet("WebSocket", webSocketServlet);
    ServletRegistration.Dynamic provisioning = environment.servlets().addServlet("Provisioning", provisioningServlet);

    websocket.addMapping("/v1/websocket/");
    websocket.setAsyncSupported(true);

    provisioning.addMapping("/v1/websocket/provisioning/");
    provisioning.setAsyncSupported(true);

    environment.admin().addTask(new SetRequestLoggingEnabledTask());
    environment.admin().addTask(new SetCrawlerAccelerationTask(accountDatabaseCrawlerCache));

///

    environment.healthChecks().register("cacheCluster", new RedisClusterHealthCheck(cacheCluster));

    environment.metrics().register(name(CpuUsageGauge.class, "cpu"), new CpuUsageGauge(3, TimeUnit.SECONDS));
    environment.metrics().register(name(FreeMemoryGauge.class, "free_memory"), new FreeMemoryGauge());
    environment.metrics().register(name(NetworkSentGauge.class, "bytes_sent"), new NetworkSentGauge());
    environment.metrics().register(name(NetworkReceivedGauge.class, "bytes_received"), new NetworkReceivedGauge());
    environment.metrics().register(name(FileDescriptorGauge.class, "fd_count"), new FileDescriptorGauge());
    environment.metrics().register(name(MaxFileDescriptorGauge.class, "max_fd_count"), new MaxFileDescriptorGauge());
    environment.metrics()
        .register(name(OperatingSystemMemoryGauge.class, "buffers"), new OperatingSystemMemoryGauge("Buffers"));
    environment.metrics()
        .register(name(OperatingSystemMemoryGauge.class, "cached"), new OperatingSystemMemoryGauge("Cached"));

    BufferPoolGauges.registerMetrics();
    GarbageCollectionGauges.registerMetrics();
  }

  private void registerExceptionMappers(Environment environment,
      WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment,
      WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment) {
    environment.jersey().register(new LoggingUnhandledExceptionMapper());
    environment.jersey().register(new IOExceptionMapper());
    environment.jersey().register(new RateLimitExceededExceptionMapper());
    environment.jersey().register(new InvalidWebsocketAddressExceptionMapper());
    environment.jersey().register(new DeviceLimitExceededExceptionMapper());
    environment.jersey().register(new RetryLaterExceptionMapper());
    environment.jersey().register(new ServerRejectedExceptionMapper());

    webSocketEnvironment.jersey().register(new LoggingUnhandledExceptionMapper());
    webSocketEnvironment.jersey().register(new IOExceptionMapper());
    webSocketEnvironment.jersey().register(new RateLimitExceededExceptionMapper());
    webSocketEnvironment.jersey().register(new InvalidWebsocketAddressExceptionMapper());
    webSocketEnvironment.jersey().register(new DeviceLimitExceededExceptionMapper());
    webSocketEnvironment.jersey().register(new RetryLaterExceptionMapper());
    webSocketEnvironment.jersey().register(new ServerRejectedExceptionMapper());

    provisioningEnvironment.jersey().register(new LoggingUnhandledExceptionMapper());
    provisioningEnvironment.jersey().register(new IOExceptionMapper());
    provisioningEnvironment.jersey().register(new RateLimitExceededExceptionMapper());
    provisioningEnvironment.jersey().register(new InvalidWebsocketAddressExceptionMapper());
    provisioningEnvironment.jersey().register(new DeviceLimitExceededExceptionMapper());
    provisioningEnvironment.jersey().register(new RetryLaterExceptionMapper());
    provisioningEnvironment.jersey().register(new ServerRejectedExceptionMapper());
  }

  private void registerCorsFilter(Environment environment) {
    FilterRegistration.Dynamic filter = environment.servlets().addFilter("CORS", CrossOriginFilter.class);
    filter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    filter.setInitParameter("allowedOrigins", "*");
    filter.setInitParameter("allowedHeaders", "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin,X-Signal-Agent");
    filter.setInitParameter("allowedMethods", "GET,PUT,POST,DELETE,OPTIONS");
    filter.setInitParameter("preflightMaxAge", "5184000");
    filter.setInitParameter("allowCredentials", "true");
  }

  public static void main(String[] args) throws Exception {
    new WhisperServerService().run(args);
  }
}
