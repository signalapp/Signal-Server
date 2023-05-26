/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.logging.LoggingOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.PolymorphicAuthDynamicFeature;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.datadog.DatadogMeterRegistry;
import java.io.ByteArrayInputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletRegistration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.server.ServerProperties;
import org.signal.event.AdminEventLogger;
import org.signal.event.GoogleCloudAdminEventLogger;
import org.signal.i18n.HeaderControlledResourceBundleLookup;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.auth.WebsocketRefreshApplicationEventListener;
import org.whispersystems.textsecuregcm.badges.ConfiguredProfileBadgeConverter;
import org.whispersystems.textsecuregcm.badges.ResourceBundleLevelTranslator;
import org.whispersystems.textsecuregcm.captcha.CaptchaChecker;
import org.whispersystems.textsecuregcm.captcha.HCaptchaClient;
import org.whispersystems.textsecuregcm.captcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.captcha.RegistrationCaptchaManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretStore;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretsModule;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AccountControllerV2;
import org.whispersystems.textsecuregcm.controllers.ArtController;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV2;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV3;
import org.whispersystems.textsecuregcm.controllers.CallLinkController;
import org.whispersystems.textsecuregcm.controllers.CertificateController;
import org.whispersystems.textsecuregcm.controllers.ChallengeController;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.controllers.DirectoryV2Controller;
import org.whispersystems.textsecuregcm.controllers.DonationController;
import org.whispersystems.textsecuregcm.controllers.KeepAliveController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.PaymentsController;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.ProvisioningController;
import org.whispersystems.textsecuregcm.controllers.RegistrationController;
import org.whispersystems.textsecuregcm.controllers.RemoteConfigController;
import org.whispersystems.textsecuregcm.controllers.SecureBackupController;
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.controllers.SecureValueRecovery2Controller;
import org.whispersystems.textsecuregcm.controllers.StickerController;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController;
import org.whispersystems.textsecuregcm.controllers.VerificationController;
import org.whispersystems.textsecuregcm.currency.CoinMarketCapClient;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.currency.FixerClient;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.filters.RemoteDeprecationFilter;
import org.whispersystems.textsecuregcm.filters.RequestStatisticsFilter;
import org.whispersystems.textsecuregcm.filters.TimestampResponseFilter;
import org.whispersystems.textsecuregcm.limits.PushChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.InvalidWebsocketAddressExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.JsonMappingExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RegistrationServiceSenderExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ServerRejectedExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.ApplicationShutdownMonitor;
import org.whispersystems.textsecuregcm.metrics.BufferPoolGauges;
import org.whispersystems.textsecuregcm.metrics.CpuUsageGauge;
import org.whispersystems.textsecuregcm.metrics.FileDescriptorGauge;
import org.whispersystems.textsecuregcm.metrics.FreeMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.GarbageCollectionGauges;
import org.whispersystems.textsecuregcm.metrics.MaxFileDescriptorGauge;
import org.whispersystems.textsecuregcm.metrics.MetricsApplicationEventListener;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.MicrometerRegistryManager;
import org.whispersystems.textsecuregcm.metrics.NetworkReceivedGauge;
import org.whispersystems.textsecuregcm.metrics.NetworkSentGauge;
import org.whispersystems.textsecuregcm.metrics.OperatingSystemMemoryGauge;
import org.whispersystems.textsecuregcm.metrics.ReportedMessageMetricsListener;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.providers.RedisClusterHealthCheck;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnPushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.FcmSender;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.push.PushLatencyManager;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.ConnectionEventLogger;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.spam.FilterSpam;
import org.whispersystems.textsecuregcm.spam.RateLimitChallengeListener;
import org.whispersystems.textsecuregcm.spam.ReportSpamTokenProvider;
import org.whispersystems.textsecuregcm.spam.ScoreThresholdProvider;
import org.whispersystems.textsecuregcm.spam.SpamFilter;
import org.whispersystems.textsecuregcm.storage.AccountCleaner;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.DeletedAccounts;
import org.whispersystems.textsecuregcm.storage.DeletedAccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.MessagePersister;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.NonNormalizedAccountCrawlerListener;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.PushChallengeDynamoDb;
import org.whispersystems.textsecuregcm.storage.PushFeedbackProcessor;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageDynamoDb;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.VerificationCodeStore;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import org.whispersystems.textsecuregcm.util.HostnameUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;
import org.whispersystems.textsecuregcm.util.logging.LoggingUnhandledExceptionMapper;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;
import org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.textsecuregcm.workers.AssignUsernameCommand;
import org.whispersystems.textsecuregcm.workers.CertificateCommand;
import org.whispersystems.textsecuregcm.workers.CheckDynamicConfigurationCommand;
import org.whispersystems.textsecuregcm.workers.CrawlAccountsCommand;
import org.whispersystems.textsecuregcm.workers.DeleteUserCommand;
import org.whispersystems.textsecuregcm.workers.MessagePersisterServiceCommand;
import org.whispersystems.textsecuregcm.workers.ScheduledApnPushNotificationSenderServiceCommand;
import org.whispersystems.textsecuregcm.workers.ServerVersionCommand;
import org.whispersystems.textsecuregcm.workers.SetRequestLoggingEnabledTask;
import org.whispersystems.textsecuregcm.workers.SetUserDiscoverabilityCommand;
import org.whispersystems.textsecuregcm.workers.UnlinkDeviceCommand;
import org.whispersystems.textsecuregcm.workers.ZkParamsCommand;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.setup.WebSocketEnvironment;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

public class WhisperServerService extends Application<WhisperServerConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(WhisperServerService.class);

  public static final String SECRETS_BUNDLE_FILE_NAME_PROPERTY = "secrets.bundle.filename";

  public static final software.amazon.awssdk.auth.credentials.AwsCredentialsProvider AWSSDK_CREDENTIALS_PROVIDER =
      AwsCredentialsProviderChain.of(
          InstanceProfileCredentialsProvider.create(),
          WebIdentityTokenFileCredentialsProvider.create());

  public static final AWSCredentialsProviderChain AWSSDK_V1_CREDENTIALS_PROVIDER_CHAIN = new AWSCredentialsProviderChain(
      com.amazonaws.auth.InstanceProfileCredentialsProvider.getInstance(),
      com.amazonaws.auth.WebIdentityTokenCredentialsProvider.create()
  );


  @Override
  public void initialize(final Bootstrap<WhisperServerConfiguration> bootstrap) {
    // `SecretStore` needs to be initialized before Dropwizard reads the main application config file.
    final String secretsBundleFileName = requireNonNull(
        System.getProperty(SECRETS_BUNDLE_FILE_NAME_PROPERTY),
        "Application requires property [%s] to be provided".formatted(SECRETS_BUNDLE_FILE_NAME_PROPERTY));
    final SecretStore secretStore = SecretStore.fromYamlFileSecretsBundle(secretsBundleFileName);
    SecretsModule.INSTANCE.setSecretStore(secretStore);

    // Initializing SystemMapper here because parsing of the main application config happens before `run()` method is called.
    SystemMapper.configureMapper(bootstrap.getObjectMapper());

    bootstrap.addCommand(new DeleteUserCommand());
    bootstrap.addCommand(new CertificateCommand());
    bootstrap.addCommand(new ZkParamsCommand());
    bootstrap.addCommand(new ServerVersionCommand());
    bootstrap.addCommand(new CheckDynamicConfigurationCommand());
    bootstrap.addCommand(new SetUserDiscoverabilityCommand());
    bootstrap.addCommand(new AssignUsernameCommand());
    bootstrap.addCommand(new UnlinkDeviceCommand());
    bootstrap.addCommand(new CrawlAccountsCommand());
    bootstrap.addCommand(new ScheduledApnPushNotificationSenderServiceCommand());
    bootstrap.addCommand(new MessagePersisterServiceCommand());
  }

  @Override
  public String getName() {
    return "whisper-server";
  }

  @Override
  public void run(WhisperServerConfiguration config, Environment environment) throws Exception {
    final Clock clock = Clock.systemUTC();
    final int availableProcessors = Runtime.getRuntime().availableProcessors();

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
          .meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(final Id id, final DistributionStatisticConfig config) {
              return defaultDistributionStatisticConfig.merge(config);
            }
          });

      Metrics.addRegistry(datadogMeterRegistry);
    }

    final boolean useSecondaryCredentialsJson = Optional.ofNullable(
            System.getenv("SIGNAL_USE_SECONDARY_CREDENTIALS_JSON"))
        .isPresent();

    environment.lifecycle().manage(new MicrometerRegistryManager(Metrics.globalRegistry));

    HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup =
        new HeaderControlledResourceBundleLookup();
    ConfiguredProfileBadgeConverter profileBadgeConverter = new ConfiguredProfileBadgeConverter(
        clock, config.getBadges(), headerControlledResourceBundleLookup);
    ResourceBundleLevelTranslator resourceBundleLevelTranslator = new ResourceBundleLevelTranslator(
        headerControlledResourceBundleLookup);

    DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbFromConfig.asyncClient(config.getDynamoDbClientConfiguration(),
        AWSSDK_CREDENTIALS_PROVIDER);

    DynamoDbClient dynamoDbClient = DynamoDbFromConfig.client(config.getDynamoDbClientConfiguration(),
        AWSSDK_CREDENTIALS_PROVIDER);

    AmazonDynamoDB deletedAccountsLockDynamoDbClient = AmazonDynamoDBClientBuilder.standard()
        .withRegion(config.getDynamoDbClientConfiguration().getRegion())
        .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(
                ((int) config.getDynamoDbClientConfiguration().getClientExecutionTimeout().toMillis()))
            .withRequestTimeout(
                (int) config.getDynamoDbClientConfiguration().getClientRequestTimeout().toMillis()))
        .withCredentials(AWSSDK_V1_CREDENTIALS_PROVIDER_CHAIN)
        .build();

    DeletedAccounts deletedAccounts = new DeletedAccounts(dynamoDbClient,
        config.getDynamoDbTables().getDeletedAccounts().getTableName());

    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new DynamicConfigurationManager<>(config.getAppConfig().getApplication(),
            config.getAppConfig().getEnvironment(),
            config.getAppConfig().getConfigurationName(),
            DynamicConfiguration.class);

    BlockingQueue<Runnable> messageDeletionQueue = new LinkedBlockingQueue<>();
    Metrics.gaugeCollectionSize(name(getClass(), "messageDeletionQueueSize"), Collections.emptyList(),
        messageDeletionQueue);
    ExecutorService messageDeletionAsyncExecutor = environment.lifecycle()
        .executorService(name(getClass(), "messageDeletionAsyncExecutor-%d")).maxThreads(16)
        .workQueue(messageDeletionQueue).build();

    Accounts accounts = new Accounts(
        dynamoDbClient,
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getAccounts().getTableName(),
        config.getDynamoDbTables().getAccounts().getPhoneNumberTableName(),
        config.getDynamoDbTables().getAccounts().getPhoneNumberIdentifierTableName(),
        config.getDynamoDbTables().getAccounts().getUsernamesTableName(),
        config.getDynamoDbTables().getAccounts().getScanPageSize());
    PhoneNumberIdentifiers phoneNumberIdentifiers = new PhoneNumberIdentifiers(dynamoDbClient,
        config.getDynamoDbTables().getPhoneNumberIdentifiers().getTableName());
    Profiles profiles = new Profiles(dynamoDbClient, dynamoDbAsyncClient,
        config.getDynamoDbTables().getProfiles().getTableName());
    Keys keys = new Keys(dynamoDbClient,
        config.getDynamoDbTables().getEcKeys().getTableName(),
        config.getDynamoDbTables().getPqKeys().getTableName(),
        config.getDynamoDbTables().getPqLastResortKeys().getTableName());
    MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(dynamoDbClient, dynamoDbAsyncClient,
        config.getDynamoDbTables().getMessages().getTableName(),
        config.getDynamoDbTables().getMessages().getExpiration(),
        messageDeletionAsyncExecutor);
    RemoteConfigs remoteConfigs = new RemoteConfigs(dynamoDbClient,
        config.getDynamoDbTables().getRemoteConfig().getTableName());
    PushChallengeDynamoDb pushChallengeDynamoDb = new PushChallengeDynamoDb(dynamoDbClient,
        config.getDynamoDbTables().getPushChallenge().getTableName());
    ReportMessageDynamoDb reportMessageDynamoDb = new ReportMessageDynamoDb(dynamoDbClient,
        config.getDynamoDbTables().getReportMessage().getTableName(),
        config.getReportMessageConfiguration().getReportTtl());
    VerificationCodeStore pendingAccounts = new VerificationCodeStore(dynamoDbClient,
        config.getDynamoDbTables().getPendingAccounts().getTableName());
    VerificationCodeStore pendingDevices = new VerificationCodeStore(dynamoDbClient,
        config.getDynamoDbTables().getPendingDevices().getTableName());
    RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        config.getDynamoDbTables().getRegistrationRecovery().getTableName(),
        config.getDynamoDbTables().getRegistrationRecovery().getExpiration(),
        dynamoDbClient,
        dynamoDbAsyncClient
    );

    final VerificationSessions verificationSessions = new VerificationSessions(dynamoDbAsyncClient,
        config.getDynamoDbTables().getVerificationSessions().getTableName(), clock);

    ClientResources redisClientResources = ClientResources.builder().build();
    ConnectionEventLogger.logConnectionEvents(redisClientResources);

    FaultTolerantRedisCluster cacheCluster             = new FaultTolerantRedisCluster("main_cache_cluster", config.getCacheClusterConfiguration(), redisClientResources);
    FaultTolerantRedisCluster messagesCluster          = new FaultTolerantRedisCluster("messages_cluster", config.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClientResources);
    FaultTolerantRedisCluster clientPresenceCluster    = new FaultTolerantRedisCluster("client_presence_cluster", config.getClientPresenceClusterConfiguration(), redisClientResources);
    FaultTolerantRedisCluster metricsCluster           = new FaultTolerantRedisCluster("metrics_cluster", config.getMetricsClusterConfiguration(), redisClientResources);
    FaultTolerantRedisCluster pushSchedulerCluster     = new FaultTolerantRedisCluster("push_scheduler", config.getPushSchedulerCluster(), redisClientResources);
    FaultTolerantRedisCluster rateLimitersCluster      = new FaultTolerantRedisCluster("rate_limiters", config.getRateLimitersCluster(), redisClientResources);

    final BlockingQueue<Runnable> keyspaceNotificationDispatchQueue = new ArrayBlockingQueue<>(100_000);
    Metrics.gaugeCollectionSize(name(getClass(), "keyspaceNotificationDispatchQueueSize"), Collections.emptyList(),
        keyspaceNotificationDispatchQueue);
    final BlockingQueue<Runnable> receiptSenderQueue = new LinkedBlockingQueue<>();
    Metrics.gaugeCollectionSize(name(getClass(), "receiptSenderQueue"), Collections.emptyList(), receiptSenderQueue);
    final BlockingQueue<Runnable> fcmSenderQueue = new LinkedBlockingQueue<>();
    Metrics.gaugeCollectionSize(name(getClass(), "fcmSenderQueue"), Collections.emptyList(), fcmSenderQueue);
    final BlockingQueue<Runnable> messageDeliveryQueue = new LinkedBlockingQueue<>();
    Metrics.gaugeCollectionSize(MetricsUtil.name(getClass(), "messageDeliveryQueue"), Collections.emptyList(),
        messageDeliveryQueue);

    ScheduledExecutorService recurringJobExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "recurringJob-%d")).threads(6).build();
    ScheduledExecutorService websocketScheduledExecutor           = environment.lifecycle().scheduledExecutorService(name(getClass(), "websocket-%d")).threads(8).build();
    ExecutorService          keyspaceNotificationDispatchExecutor = environment.lifecycle().executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(16).workQueue(keyspaceNotificationDispatchQueue).build();
    ExecutorService          apnSenderExecutor                    = environment.lifecycle().executorService(name(getClass(), "apnSender-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService fcmSenderExecutor = environment.lifecycle().executorService(name(getClass(), "fcmSender-%d"))
        .maxThreads(32).minThreads(32).workQueue(fcmSenderQueue).build();
    ExecutorService secureValueRecoveryServiceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "secureValueRecoveryService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService storageServiceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "storageService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService          accountDeletionExecutor              = environment.lifecycle().executorService(name(getClass(), "accountCleaner-%d")).maxThreads(16).minThreads(16).build();

    Scheduler messageDeliveryScheduler = Schedulers.fromExecutorService(
            ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                    environment.lifecycle().executorService(name(getClass(), "messageDelivery-%d"))
                            .minThreads(20)
                            .maxThreads(20)
                            .workQueue(messageDeliveryQueue)
                            .build(),
                    MetricsUtil.name(getClass(), "messageDeliveryExecutor"), MetricsUtil.PREFIX),
            "messageDelivery");
    // TODO: generally speaking this is a DynamoDB I/O executor for the accounts table; we should eventually have a general executor for speaking to the accounts table, but most of the server is still synchronous so this isn't widely useful yet
    ExecutorService batchIdentityCheckExecutor = environment.lifecycle().executorService(name(getClass(), "batchIdentityCheck-%d")).minThreads(32).maxThreads(32).build();
    ExecutorService multiRecipientMessageExecutor = environment.lifecycle()
        .executorService(name(getClass(), "multiRecipientMessage-%d")).minThreads(64).maxThreads(64).build();
    ExecutorService subscriptionProcessorExecutor = environment.lifecycle()
        .executorService(name(getClass(), "subscriptionProcessor-%d"))
        .maxThreads(availableProcessors)  // mostly this is IO bound so tying to number of processors is tenuous at best
        .minThreads(availableProcessors)  // mostly this is IO bound so tying to number of processors is tenuous at best
        .allowCoreThreadTimeOut(true).
        build();
    ExecutorService receiptSenderExecutor = environment.lifecycle()
        .executorService(name(getClass(), "receiptSender-%d"))
        .maxThreads(2)
        .minThreads(2)
        .workQueue(receiptSenderQueue)
        .rejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy())
        .build();
    ExecutorService registrationCallbackExecutor = environment.lifecycle()
        .executorService(name(getClass(), "registration-%d"))
        .maxThreads(2)
        .minThreads(2)
        .build();

    final AdminEventLogger adminEventLogger = new GoogleCloudAdminEventLogger(
        LoggingOptions.newBuilder().setProjectId(config.getAdminEventLoggingConfiguration().projectId())
            .setCredentials(GoogleCredentials.fromStream(new ByteArrayInputStream(
                useSecondaryCredentialsJson
                    ? config.getAdminEventLoggingConfiguration().secondaryCredentials().getBytes(StandardCharsets.UTF_8)
                    : config.getAdminEventLoggingConfiguration().credentials().getBytes(StandardCharsets.UTF_8))))
            .build().getService(),
        config.getAdminEventLoggingConfiguration().projectId(),
        config.getAdminEventLoggingConfiguration().logName());

    StripeManager stripeManager = new StripeManager(config.getStripe().apiKey().value(), subscriptionProcessorExecutor,
        config.getStripe().idempotencyKeyGenerator().value(), config.getStripe().boostDescription(), config.getStripe()
        .supportedCurrencies());
    BraintreeManager braintreeManager = new BraintreeManager(config.getBraintree().merchantId(),
        config.getBraintree().publicKey(), config.getBraintree().privateKey().value(), config.getBraintree().environment(),
        config.getBraintree().supportedCurrencies(), config.getBraintree().merchantAccounts(),
        config.getBraintree().graphqlUrl(), config.getBraintree().circuitBreaker(), subscriptionProcessorExecutor);

    ExternalServiceCredentialsGenerator directoryV2CredentialsGenerator = DirectoryV2Controller.credentialsGenerator(
        config.getDirectoryV2Configuration().getDirectoryV2ClientConfiguration());
    ExternalServiceCredentialsGenerator storageCredentialsGenerator = SecureStorageController.credentialsGenerator(
        config.getSecureStorageServiceConfiguration());
    ExternalServiceCredentialsGenerator backupCredentialsGenerator = SecureBackupController.credentialsGenerator(
        config.getSecureBackupServiceConfiguration());
    ExternalServiceCredentialsGenerator paymentsCredentialsGenerator = PaymentsController.credentialsGenerator(
        config.getPaymentsServiceConfiguration());
    ExternalServiceCredentialsGenerator artCredentialsGenerator = ArtController.credentialsGenerator(
        config.getArtServiceConfiguration());
    ExternalServiceCredentialsGenerator svr2CredentialsGenerator = SecureValueRecovery2Controller.credentialsGenerator(
        config.getSvr2Configuration());

    dynamicConfigurationManager.start();

    ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(
        dynamicConfigurationManager);
    RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = new RegistrationRecoveryPasswordsManager(
        registrationRecoveryPasswords);
    UsernameHashZkProofVerifier usernameHashZkProofVerifier = new UsernameHashZkProofVerifier();

    RegistrationServiceClient registrationServiceClient = new RegistrationServiceClient(
        config.getRegistrationServiceConfiguration().host(),
        config.getRegistrationServiceConfiguration().port(),
        useSecondaryCredentialsJson
            ? config.getRegistrationServiceConfiguration().secondaryCredentialConfigurationJson()
            : config.getRegistrationServiceConfiguration().credentialConfigurationJson(),
        config.getRegistrationServiceConfiguration().identityTokenAudience(),
        config.getRegistrationServiceConfiguration().registrationCaCertificate(),
        registrationCallbackExecutor);
    SecureBackupClient secureBackupClient = new SecureBackupClient(backupCredentialsGenerator,
        secureValueRecoveryServiceExecutor, config.getSecureBackupServiceConfiguration());
    SecureValueRecovery2Client secureValueRecovery2Client = new SecureValueRecovery2Client(svr2CredentialsGenerator,
        secureValueRecoveryServiceExecutor, config.getSvr2Configuration());
    SecureStorageClient secureStorageClient = new SecureStorageClient(storageCredentialsGenerator,
        storageServiceExecutor, config.getSecureStorageServiceConfiguration());
    ClientPresenceManager clientPresenceManager = new ClientPresenceManager(clientPresenceCluster, recurringJobExecutor,
        keyspaceNotificationDispatchExecutor);
    StoredVerificationCodeManager pendingAccountsManager = new StoredVerificationCodeManager(pendingAccounts);
    StoredVerificationCodeManager pendingDevicesManager = new StoredVerificationCodeManager(pendingDevices);
    ProfilesManager profilesManager = new ProfilesManager(profiles, cacheCluster);
    MessagesCache messagesCache = new MessagesCache(messagesCluster, messagesCluster,
        keyspaceNotificationDispatchExecutor, messageDeliveryScheduler, messageDeletionAsyncExecutor, clock);
    PushLatencyManager pushLatencyManager = new PushLatencyManager(metricsCluster, dynamicConfigurationManager);
    ReportMessageManager reportMessageManager = new ReportMessageManager(reportMessageDynamoDb, rateLimitersCluster,
        config.getReportMessageConfiguration().getCounterTtl());
    MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, reportMessageManager,
        messageDeletionAsyncExecutor);
    DeletedAccountsManager deletedAccountsManager = new DeletedAccountsManager(deletedAccounts,
        deletedAccountsLockDynamoDbClient, config.getDynamoDbTables().getDeletedAccountsLock().getTableName());
    AccountsManager accountsManager = new AccountsManager(accounts, phoneNumberIdentifiers, cacheCluster,
        deletedAccountsManager, keys, messagesManager, profilesManager,
        pendingAccountsManager, secureStorageClient, secureBackupClient, secureValueRecovery2Client,
        clientPresenceManager,
        experimentEnrollmentManager, registrationRecoveryPasswordsManager, clock);
    RemoteConfigsManager remoteConfigsManager = new RemoteConfigsManager(remoteConfigs);
    APNSender apnSender = new APNSender(apnSenderExecutor, config.getApnConfiguration());
    FcmSender fcmSender = new FcmSender(fcmSenderExecutor, config.getFcmConfiguration().credentials().value());
    ApnPushNotificationScheduler apnPushNotificationScheduler = new ApnPushNotificationScheduler(pushSchedulerCluster,
        apnSender, accountsManager, Optional.empty(), dynamicConfigurationManager);
    PushNotificationManager pushNotificationManager = new PushNotificationManager(accountsManager, apnSender, fcmSender,
        apnPushNotificationScheduler, pushLatencyManager);
    RateLimiters rateLimiters = RateLimiters.createAndValidate(config.getLimitsConfiguration(),
        dynamicConfigurationManager, rateLimitersCluster);
    ProvisioningManager provisioningManager = new ProvisioningManager(config.getPubsubCacheConfiguration().getUri(),
        redisClientResources, config.getPubsubCacheConfiguration().getTimeout(),
        config.getPubsubCacheConfiguration().getCircuitBreakerConfiguration());
    IssuedReceiptsManager issuedReceiptsManager = new IssuedReceiptsManager(
        config.getDynamoDbTables().getIssuedReceipts().getTableName(),
        config.getDynamoDbTables().getIssuedReceipts().getExpiration(),
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getIssuedReceipts().getGenerator());
    RedeemedReceiptsManager redeemedReceiptsManager = new RedeemedReceiptsManager(clock,
        config.getDynamoDbTables().getRedeemedReceipts().getTableName(),
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getRedeemedReceipts().getExpiration());
    SubscriptionManager subscriptionManager = new SubscriptionManager(
        config.getDynamoDbTables().getSubscriptions().getTableName(), dynamoDbAsyncClient);

    final RegistrationLockVerificationManager registrationLockVerificationManager = new RegistrationLockVerificationManager(
        accountsManager, clientPresenceManager, backupCredentialsGenerator, svr2CredentialsGenerator, registrationRecoveryPasswordsManager, pushNotificationManager, rateLimiters);
    final PhoneVerificationTokenManager phoneVerificationTokenManager = new PhoneVerificationTokenManager(
        registrationServiceClient, registrationRecoveryPasswordsManager);

    final ReportedMessageMetricsListener reportedMessageMetricsListener = new ReportedMessageMetricsListener(
        accountsManager);
    reportMessageManager.addListener(reportedMessageMetricsListener);

    final AccountAuthenticator accountAuthenticator = new AccountAuthenticator(accountsManager);
    final DisabledPermittedAccountAuthenticator disabledPermittedAccountAuthenticator = new DisabledPermittedAccountAuthenticator(
        accountsManager);

    final MessageSender messageSender = new MessageSender(clientPresenceManager, messagesManager,
        pushNotificationManager,
        pushLatencyManager);
    final ReceiptSender receiptSender = new ReceiptSender(accountsManager, messageSender, receiptSenderExecutor);
    final TurnTokenGenerator turnTokenGenerator = new TurnTokenGenerator(dynamicConfigurationManager);

    RecaptchaClient recaptchaClient = new RecaptchaClient(
        config.getRecaptchaConfiguration().projectPath(),
        useSecondaryCredentialsJson
            ? config.getRecaptchaConfiguration().secondaryCredentialConfigurationJson()
            : config.getRecaptchaConfiguration().credentialConfigurationJson(),
        dynamicConfigurationManager);
    HttpClient hcaptchaHttpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(10)).build();
    HCaptchaClient hCaptchaClient = new HCaptchaClient(config.getHCaptchaConfiguration().apiKey().value(), hcaptchaHttpClient,
        dynamicConfigurationManager);
    CaptchaChecker captchaChecker = new CaptchaChecker(List.of(recaptchaClient, hCaptchaClient));

    PushChallengeManager pushChallengeManager = new PushChallengeManager(pushNotificationManager,
        pushChallengeDynamoDb);
    RateLimitChallengeManager rateLimitChallengeManager = new RateLimitChallengeManager(pushChallengeManager,
        captchaChecker, rateLimiters);

    MessagePersister messagePersister = new MessagePersister(messagesCache, messagesManager, accountsManager,
        dynamicConfigurationManager, Duration.ofMinutes(config.getMessageCacheConfiguration().getPersistDelayMinutes()),
        Optional.empty());
    ChangeNumberManager changeNumberManager = new ChangeNumberManager(messageSender, accountsManager);

    AccountDatabaseCrawlerCache accountCleanerAccountDatabaseCrawlerCache =
        new AccountDatabaseCrawlerCache(cacheCluster, AccountDatabaseCrawlerCache.ACCOUNT_CLEANER_PREFIX);
    AccountDatabaseCrawler accountCleanerAccountDatabaseCrawler = new AccountDatabaseCrawler("Account cleaner crawler",
        accountsManager,
        accountCleanerAccountDatabaseCrawlerCache,
        List.of(new AccountCleaner(accountsManager, accountDeletionExecutor)),
        config.getAccountDatabaseCrawlerConfiguration().getChunkSize(),
        dynamicConfigurationManager
    );

    // TODO listeners must be ordered so that ones that directly update accounts come last, so that read-only ones are not working with stale data
    final List<AccountDatabaseCrawlerListener> accountDatabaseCrawlerListeners = List.of(
        new NonNormalizedAccountCrawlerListener(accountsManager, metricsCluster),
        // PushFeedbackProcessor may update device properties
        new PushFeedbackProcessor(accountsManager));

    AccountDatabaseCrawlerCache accountDatabaseCrawlerCache = new AccountDatabaseCrawlerCache(cacheCluster,
        AccountDatabaseCrawlerCache.GENERAL_PURPOSE_PREFIX);
    AccountDatabaseCrawler accountDatabaseCrawler = new AccountDatabaseCrawler("General-purpose account crawler",
        accountsManager,
        accountDatabaseCrawlerCache, accountDatabaseCrawlerListeners,
        config.getAccountDatabaseCrawlerConfiguration().getChunkSize(),
        dynamicConfigurationManager
    );

    HttpClient currencyClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(10)).build();
    FixerClient fixerClient = new FixerClient(currencyClient, config.getPaymentsServiceConfiguration().fixerApiKey().value());
    CoinMarketCapClient coinMarketCapClient = new CoinMarketCapClient(currencyClient, config.getPaymentsServiceConfiguration().coinMarketCapApiKey().value(), config.getPaymentsServiceConfiguration().coinMarketCapCurrencyIds());
    CurrencyConversionManager currencyManager = new CurrencyConversionManager(fixerClient, coinMarketCapClient,
        cacheCluster, config.getPaymentsServiceConfiguration().paymentCurrencies(), Clock.systemUTC());

    environment.lifecycle().manage(apnSender);
    environment.lifecycle().manage(apnPushNotificationScheduler);
    environment.lifecycle().manage(provisioningManager);
    environment.lifecycle().manage(accountDatabaseCrawler);
    environment.lifecycle().manage(accountCleanerAccountDatabaseCrawler);
    environment.lifecycle().manage(messagesCache);
    environment.lifecycle().manage(messagePersister);
    environment.lifecycle().manage(clientPresenceManager);
    environment.lifecycle().manage(currencyManager);
    environment.lifecycle().manage(registrationServiceClient);

    final RegistrationCaptchaManager registrationCaptchaManager = new RegistrationCaptchaManager(captchaChecker,
        rateLimiters, config.getTestDevices(), dynamicConfigurationManager);

    StaticCredentialsProvider cdnCredentialsProvider = StaticCredentialsProvider
        .create(AwsBasicCredentials.create(
            config.getCdnConfiguration().accessKey().value(),
            config.getCdnConfiguration().accessSecret().value()));
    S3Client cdnS3Client = S3Client.builder()
        .credentialsProvider(cdnCredentialsProvider)
        .region(Region.of(config.getCdnConfiguration().region()))
        .build();
    PostPolicyGenerator profileCdnPolicyGenerator = new PostPolicyGenerator(config.getCdnConfiguration().region(),
        config.getCdnConfiguration().bucket(), config.getCdnConfiguration().accessKey().value());
    PolicySigner profileCdnPolicySigner = new PolicySigner(config.getCdnConfiguration().accessSecret().value(),
        config.getCdnConfiguration().region());

    ServerSecretParams zkSecretParams = new ServerSecretParams(config.getZkConfig().serverSecret().value());
    GenericServerSecretParams genericZkSecretParams = new GenericServerSecretParams(config.getGenericZkConfig().serverSecret().value());
    ServerZkProfileOperations zkProfileOperations = new ServerZkProfileOperations(zkSecretParams);
    ServerZkAuthOperations zkAuthOperations = new ServerZkAuthOperations(zkSecretParams);
    ServerZkReceiptOperations zkReceiptOperations = new ServerZkReceiptOperations(zkSecretParams);

    AuthFilter<BasicCredentials, AuthenticatedAccount> accountAuthFilter = new BasicCredentialAuthFilter.Builder<AuthenticatedAccount>().setAuthenticator(
        accountAuthenticator).buildAuthFilter();
    AuthFilter<BasicCredentials, DisabledPermittedAuthenticatedAccount> disabledPermittedAccountAuthFilter = new BasicCredentialAuthFilter.Builder<DisabledPermittedAuthenticatedAccount>().setAuthenticator(
        disabledPermittedAccountAuthenticator).buildAuthFilter();

    environment.servlets()
        .addFilter("RemoteDeprecationFilter", new RemoteDeprecationFilter(dynamicConfigurationManager))
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");

    environment.jersey().register(new RequestStatisticsFilter(TrafficSource.HTTP));
    environment.jersey().register(MultiRecipientMessageProvider.class);
    environment.jersey().register(new MetricsApplicationEventListener(TrafficSource.HTTP));
    environment.jersey()
        .register(new PolymorphicAuthDynamicFeature<>(ImmutableMap.of(AuthenticatedAccount.class, accountAuthFilter,
            DisabledPermittedAuthenticatedAccount.class, disabledPermittedAccountAuthFilter)));
    environment.jersey().register(new PolymorphicAuthValueFactoryProvider.Binder<>(
        ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)));
    environment.jersey().register(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager));
    environment.jersey().register(new TimestampResponseFilter());

    ///
    WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment = new WebSocketEnvironment<>(environment,
        config.getWebSocketConfiguration(), 90000);
    webSocketEnvironment.setAuthenticator(new WebSocketAccountAuthenticator(accountAuthenticator));
    webSocketEnvironment.setConnectListener(
        new AuthenticatedConnectListener(receiptSender, messagesManager, pushNotificationManager,
            clientPresenceManager, websocketScheduledExecutor, messageDeliveryScheduler));
    webSocketEnvironment.jersey()
        .register(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager));
    webSocketEnvironment.jersey().register(new RequestStatisticsFilter(TrafficSource.WEBSOCKET));
    webSocketEnvironment.jersey().register(MultiRecipientMessageProvider.class);
    webSocketEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET));
    webSocketEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));

    // these should be common, but use @Auth DisabledPermittedAccount, which isn’t supported yet on websocket
    environment.jersey().register(
        new AccountController(pendingAccountsManager, accountsManager, rateLimiters,
            registrationServiceClient, dynamicConfigurationManager, turnTokenGenerator,
            registrationCaptchaManager, pushNotificationManager, changeNumberManager,
            registrationLockVerificationManager, registrationRecoveryPasswordsManager, usernameHashZkProofVerifier, clock));

    environment.jersey().register(new KeysController(rateLimiters, keys, accountsManager));

    boolean registeredSpamFilter = false;
    ReportSpamTokenProvider reportSpamTokenProvider = null;

    for (final SpamFilter filter : ServiceLoader.load(SpamFilter.class)) {
      if (filter.getClass().isAnnotationPresent(FilterSpam.class)) {
        try {
          filter.configure(config.getSpamFilterConfiguration().getEnvironment());

          ReportSpamTokenProvider thisProvider = filter.getReportSpamTokenProvider();
          if (reportSpamTokenProvider == null) {
            reportSpamTokenProvider = thisProvider;
          } else if (thisProvider != null) {
            log.info("Multiple spam report token providers found. Using the first.");
          }

          filter.getReportedMessageListeners().forEach(reportMessageManager::addListener);

          environment.lifecycle().manage(filter);
          environment.jersey().register(filter);
          webSocketEnvironment.jersey().register(filter);

          log.info("Registered spam filter: {}", filter.getClass().getName());
          registeredSpamFilter = true;
        } catch (final Exception e) {
          log.warn("Failed to register spam filter: {}", filter.getClass().getName(), e);
        }
      } else {
        log.warn("Spam filter {} not annotated with @FilterSpam and will not be installed",
            filter.getClass().getName());
      }

      if (filter instanceof RateLimitChallengeListener) {
        log.info("Registered rate limit challenge listener: {}", filter.getClass().getName());
        rateLimitChallengeManager.addListener((RateLimitChallengeListener) filter);
      }
    }

    if (!registeredSpamFilter) {
      log.warn("No spam filters installed");
    }

    if (reportSpamTokenProvider == null) {
      log.warn("No spam-reporting token providers found; using default (no-op) provider as a default");
      reportSpamTokenProvider = ReportSpamTokenProvider.noop();
    }

    final List<Object> commonControllers = Lists.newArrayList(
        new AccountControllerV2(accountsManager, changeNumberManager, phoneVerificationTokenManager,
            registrationLockVerificationManager, rateLimiters),
        new ArtController(rateLimiters, artCredentialsGenerator),
        new AttachmentControllerV2(rateLimiters, config.getAwsAttachmentsConfiguration().accessKey().value(), config.getAwsAttachmentsConfiguration().accessSecret().value(), config.getAwsAttachmentsConfiguration().region(), config.getAwsAttachmentsConfiguration().bucket()),
        new AttachmentControllerV3(rateLimiters, config.getGcpAttachmentsConfiguration().domain(), config.getGcpAttachmentsConfiguration().email(), config.getGcpAttachmentsConfiguration().maxSizeInBytes(), config.getGcpAttachmentsConfiguration().pathPrefix(), config.getGcpAttachmentsConfiguration().rsaSigningKey().value()),
        new CallLinkController(rateLimiters, genericZkSecretParams),
        new CertificateController(new CertificateGenerator(config.getDeliveryCertificate().certificate().value(), config.getDeliveryCertificate().ecPrivateKey(), config.getDeliveryCertificate().expiresDays()), zkAuthOperations, genericZkSecretParams, clock),
        new ChallengeController(rateLimitChallengeManager),
        new DeviceController(pendingDevicesManager, accountsManager, messagesManager, keys, rateLimiters, config.getMaxDevices()),
        new DirectoryV2Controller(directoryV2CredentialsGenerator),
        new DonationController(clock, zkReceiptOperations, redeemedReceiptsManager, accountsManager, config.getBadges(),
            ReceiptCredentialPresentation::new),
        new MessageController(rateLimiters, messageSender, receiptSender, accountsManager, deletedAccountsManager,
            messagesManager, pushNotificationManager, reportMessageManager, multiRecipientMessageExecutor,
            messageDeliveryScheduler, reportSpamTokenProvider),
        new PaymentsController(currencyManager, paymentsCredentialsGenerator),
        new ProfileController(clock, rateLimiters, accountsManager, profilesManager, dynamicConfigurationManager,
            profileBadgeConverter, config.getBadges(), cdnS3Client, profileCdnPolicyGenerator, profileCdnPolicySigner,
            config.getCdnConfiguration().bucket(), zkProfileOperations, batchIdentityCheckExecutor),
        new ProvisioningController(rateLimiters, provisioningManager),
        new RegistrationController(accountsManager, phoneVerificationTokenManager, registrationLockVerificationManager,
            keys, rateLimiters),
        new RemoteConfigController(remoteConfigsManager, adminEventLogger,
            config.getRemoteConfigConfiguration().authorizedTokens().value(),
            config.getRemoteConfigConfiguration().globalConfig()),
        new SecureBackupController(backupCredentialsGenerator, accountsManager),
        new SecureStorageController(storageCredentialsGenerator),
        new SecureValueRecovery2Controller(svr2CredentialsGenerator, accountsManager),
        new StickerController(rateLimiters, config.getCdnConfiguration().accessKey().value(),
            config.getCdnConfiguration().accessSecret().value(), config.getCdnConfiguration().region(),
            config.getCdnConfiguration().bucket()),
        new VerificationController(registrationServiceClient, new VerificationSessionManager(verificationSessions),
            pushNotificationManager, registrationCaptchaManager, registrationRecoveryPasswordsManager, rateLimiters,
            accountsManager, clock)
    );
    if (config.getSubscription() != null && config.getOneTimeDonations() != null) {
      commonControllers.add(new SubscriptionController(clock, config.getSubscription(), config.getOneTimeDonations(),
          subscriptionManager, stripeManager, braintreeManager, zkReceiptOperations, issuedReceiptsManager, profileBadgeConverter,
          resourceBundleLevelTranslator));
    }

    for (Object controller : commonControllers) {
      environment.jersey().register(controller);
      webSocketEnvironment.jersey().register(controller);
    }


    WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment = new WebSocketEnvironment<>(environment,
        webSocketEnvironment.getRequestLog(), 60000);
    provisioningEnvironment.jersey().register(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager));
    provisioningEnvironment.setConnectListener(new ProvisioningConnectListener(provisioningManager));
    provisioningEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET));
    provisioningEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));

    registerCorsFilter(environment);
    registerExceptionMappers(environment, webSocketEnvironment, provisioningEnvironment);
    registerProviders(environment, webSocketEnvironment, provisioningEnvironment);

    environment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
    webSocketEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
    provisioningEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);

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

    environment.healthChecks().register("cacheCluster", new RedisClusterHealthCheck(cacheCluster));

    environment.lifecycle().manage(new ApplicationShutdownMonitor(Metrics.globalRegistry));

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


  private void registerProviders(Environment environment,
      WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment,
      WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment) {
    environment.jersey().register(ScoreThresholdProvider.ScoreThresholdFeature.class);
    webSocketEnvironment.jersey().register(ScoreThresholdProvider.ScoreThresholdFeature.class);
    provisioningEnvironment.jersey().register(ScoreThresholdProvider.ScoreThresholdFeature.class);
  }

  private void registerExceptionMappers(Environment environment,
      WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment,
      WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment) {

    List.of(
        new LoggingUnhandledExceptionMapper(),
        new CompletionExceptionMapper(),
        new IOExceptionMapper(),
        new RateLimitExceededExceptionMapper(),
        new InvalidWebsocketAddressExceptionMapper(),
        new DeviceLimitExceededExceptionMapper(),
        new ServerRejectedExceptionMapper(),
        new ImpossiblePhoneNumberExceptionMapper(),
        new NonNormalizedPhoneNumberExceptionMapper(),
        new RegistrationServiceSenderExceptionMapper(),
        new JsonMappingExceptionMapper()
    ).forEach(exceptionMapper -> {
      environment.jersey().register(exceptionMapper);
      webSocketEnvironment.jersey().register(exceptionMapper);
      provisioningEnvironment.jersey().register(exceptionMapper);
    });
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
