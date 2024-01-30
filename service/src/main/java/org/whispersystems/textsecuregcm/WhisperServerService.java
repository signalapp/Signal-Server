/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.Lists;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.core.Application;
import io.dropwizard.core.server.DefaultServerFactory;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jetty.HttpsConnectorFactory;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.grpc.MetricCollectingServerInterceptor;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
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
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletRegistration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.glassfish.jersey.server.ServerProperties;
import org.signal.i18n.HeaderControlledResourceBundleLookup;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.auth.WebsocketRefreshApplicationEventListener;
import org.whispersystems.textsecuregcm.auth.grpc.BasicCredentialAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupsDb;
import org.whispersystems.textsecuregcm.backup.Cdn3BackupCredentialGenerator;
import org.whispersystems.textsecuregcm.backup.Cdn3RemoteStorageManager;
import org.whispersystems.textsecuregcm.badges.ConfiguredProfileBadgeConverter;
import org.whispersystems.textsecuregcm.badges.ResourceBundleLevelTranslator;
import org.whispersystems.textsecuregcm.captcha.CaptchaChecker;
import org.whispersystems.textsecuregcm.captcha.HCaptchaClient;
import org.whispersystems.textsecuregcm.captcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.captcha.RegistrationCaptchaManager;
import org.whispersystems.textsecuregcm.captcha.ShortCodeExpander;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretStore;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretsModule;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AccountControllerV2;
import org.whispersystems.textsecuregcm.controllers.ArchiveController;
import org.whispersystems.textsecuregcm.controllers.ArtController;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV2;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV3;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV4;
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
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.controllers.SecureValueRecovery2Controller;
import org.whispersystems.textsecuregcm.controllers.SecureValueRecovery3Controller;
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
import org.whispersystems.textsecuregcm.grpc.AcceptLanguageInterceptor;
import org.whispersystems.textsecuregcm.grpc.AccountsAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.AccountsGrpcService;
import org.whispersystems.textsecuregcm.grpc.ErrorMappingInterceptor;
import org.whispersystems.textsecuregcm.grpc.ExternalServiceCredentialsAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.ExternalServiceCredentialsGrpcService;
import org.whispersystems.textsecuregcm.grpc.GrpcServerManagedWrapper;
import org.whispersystems.textsecuregcm.grpc.KeysAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.KeysGrpcService;
import org.whispersystems.textsecuregcm.grpc.PaymentsGrpcService;
import org.whispersystems.textsecuregcm.grpc.ProfileAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.ProfileGrpcService;
import org.whispersystems.textsecuregcm.grpc.UserAgentInterceptor;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.PushChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.GrpcStatusRuntimeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.InvalidWebsocketAddressExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.JsonMappingExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RegistrationServiceSenderExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ServerRejectedExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.SubscriptionProcessorExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.MetricsApplicationEventListener;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
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
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.spam.FilterSpam;
import org.whispersystems.textsecuregcm.spam.PushChallengeConfigProvider;
import org.whispersystems.textsecuregcm.spam.RateLimitChallengeListener;
import org.whispersystems.textsecuregcm.spam.ReportSpamTokenProvider;
import org.whispersystems.textsecuregcm.spam.ScoreThresholdProvider;
import org.whispersystems.textsecuregcm.spam.SenderOverrideProvider;
import org.whispersystems.textsecuregcm.spam.SpamFilter;
import org.whispersystems.textsecuregcm.storage.AccountLockManager;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeys;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.ClientReleases;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.OneTimeDonationsManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.PushChallengeDynamoDb;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageDynamoDb;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import org.whispersystems.textsecuregcm.subscriptions.BankMandateTranslator;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import org.whispersystems.textsecuregcm.util.ManagedAwsCrt;
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
import org.whispersystems.textsecuregcm.workers.DeleteUserCommand;
import org.whispersystems.textsecuregcm.workers.MessagePersisterServiceCommand;
import org.whispersystems.textsecuregcm.workers.ProcessPushNotificationFeedbackCommand;
import org.whispersystems.textsecuregcm.workers.RemoveExpiredAccountsCommand;
import org.whispersystems.textsecuregcm.workers.RemoveExpiredLinkedDevicesCommand;
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
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

public class WhisperServerService extends Application<WhisperServerConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(WhisperServerService.class);

  public static final String SECRETS_BUNDLE_FILE_NAME_PROPERTY = "secrets.bundle.filename";

  public static final software.amazon.awssdk.auth.credentials.AwsCredentialsProvider AWSSDK_CREDENTIALS_PROVIDER =
      WebIdentityTokenFileCredentialsProvider.create();

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

    // Enable variable substitution with environment variables
    // https://www.dropwizard.io/en/stable/manual/core.html#environment-variables
    final EnvironmentVariableSubstitutor substitutor = new EnvironmentVariableSubstitutor(true);
    final SubstitutingSourceProvider provider =
        new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(), substitutor);
    bootstrap.setConfigurationSourceProvider(provider);

    bootstrap.addCommand(new DeleteUserCommand());
    bootstrap.addCommand(new CertificateCommand());
    bootstrap.addCommand(new ZkParamsCommand());
    bootstrap.addCommand(new ServerVersionCommand());
    bootstrap.addCommand(new CheckDynamicConfigurationCommand());
    bootstrap.addCommand(new SetUserDiscoverabilityCommand());
    bootstrap.addCommand(new AssignUsernameCommand());
    bootstrap.addCommand(new UnlinkDeviceCommand());
    bootstrap.addCommand(new ScheduledApnPushNotificationSenderServiceCommand());
    bootstrap.addCommand(new MessagePersisterServiceCommand());
    bootstrap.addCommand(new RemoveExpiredAccountsCommand(Clock.systemUTC()));
    bootstrap.addCommand(new ProcessPushNotificationFeedbackCommand(Clock.systemUTC()));
    bootstrap.addCommand(new RemoveExpiredLinkedDevicesCommand());
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

    MetricsUtil.configureRegistries(config, environment);

    final boolean useRemoteAddress = Optional.ofNullable(
            System.getenv("SIGNAL_USE_REMOTE_ADDRESS"))
        .isPresent();

    if (config.getServerFactory() instanceof DefaultServerFactory defaultServerFactory) {
      defaultServerFactory.getApplicationConnectors()
          .forEach(connectorFactory -> {
            if (connectorFactory instanceof HttpsConnectorFactory h) {
              h.setKeyStorePassword(config.getTlsKeyStoreConfiguration().password().value());
            }
          });
    }

    HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup =
        new HeaderControlledResourceBundleLookup();
    ConfiguredProfileBadgeConverter profileBadgeConverter = new ConfiguredProfileBadgeConverter(
        clock, config.getBadges(), headerControlledResourceBundleLookup);
    ResourceBundleLevelTranslator resourceBundleLevelTranslator = new ResourceBundleLevelTranslator(
        headerControlledResourceBundleLookup);
    BankMandateTranslator bankMandateTranslator = new BankMandateTranslator(headerControlledResourceBundleLookup);

    environment.lifecycle().manage(new ManagedAwsCrt());
    DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbFromConfig.asyncClient(config.getDynamoDbClientConfiguration(),
        AWSSDK_CREDENTIALS_PROVIDER);

    DynamoDbClient dynamoDbClient = DynamoDbFromConfig.client(config.getDynamoDbClientConfiguration(),
        AWSSDK_CREDENTIALS_PROVIDER);

    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new DynamicConfigurationManager<>(config.getAppConfig().getApplication(),
            config.getAppConfig().getEnvironment(),
            config.getAppConfig().getConfigurationName(),
            DynamicConfiguration.class);

    BlockingQueue<Runnable> messageDeletionQueue = new LinkedBlockingQueue<>();
    Metrics.gaugeCollectionSize(name(getClass(), "messageDeletionQueueSize"), Collections.emptyList(),
        messageDeletionQueue);
    ExecutorService messageDeletionAsyncExecutor = environment.lifecycle()
        .executorService(name(getClass(), "messageDeletionAsyncExecutor-%d"))
        .minThreads(2)
        .maxThreads(2)
        .allowCoreThreadTimeOut(true)
        .workQueue(messageDeletionQueue).build();

    Accounts accounts = new Accounts(
        dynamoDbClient,
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getAccounts().getTableName(),
        config.getDynamoDbTables().getAccounts().getPhoneNumberTableName(),
        config.getDynamoDbTables().getAccounts().getPhoneNumberIdentifierTableName(),
        config.getDynamoDbTables().getAccounts().getUsernamesTableName(),
        config.getDynamoDbTables().getDeletedAccounts().getTableName());
    ClientReleases clientReleases = new ClientReleases(dynamoDbAsyncClient,
        config.getDynamoDbTables().getClientReleases().getTableName());
    PhoneNumberIdentifiers phoneNumberIdentifiers = new PhoneNumberIdentifiers(dynamoDbClient,
        config.getDynamoDbTables().getPhoneNumberIdentifiers().getTableName());
    Profiles profiles = new Profiles(dynamoDbClient, dynamoDbAsyncClient,
        config.getDynamoDbTables().getProfiles().getTableName());
    KeysManager keysManager = new KeysManager(
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getEcKeys().getTableName(),
        config.getDynamoDbTables().getKemKeys().getTableName(),
        config.getDynamoDbTables().getEcSignedPreKeys().getTableName(),
        config.getDynamoDbTables().getKemLastResortKeys().getTableName()
    );
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
    RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        config.getDynamoDbTables().getRegistrationRecovery().getTableName(),
        config.getDynamoDbTables().getRegistrationRecovery().getExpiration(),
        dynamoDbClient,
        dynamoDbAsyncClient
    );
    ClientPublicKeys clientPublicKeys =
        new ClientPublicKeys(dynamoDbAsyncClient, config.getDynamoDbTables().getClientPublicKeys().getTableName());

    final VerificationSessions verificationSessions = new VerificationSessions(dynamoDbAsyncClient,
        config.getDynamoDbTables().getVerificationSessions().getTableName(), clock);

    final ClientResources redisClientResources = ClientResources.builder()
        .commandLatencyRecorder(new MicrometerCommandLatencyRecorder(Metrics.globalRegistry, MicrometerOptions.builder().build()))
        .build();

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
    ScheduledExecutorService websocketScheduledExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "websocket-%d")).threads(8).build();
    ExecutorService keyspaceNotificationDispatchExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
        environment.lifecycle()
            .executorService(name(getClass(), "keyspaceNotification-%d"))
            .maxThreads(16)
            .workQueue(keyspaceNotificationDispatchQueue)
            .build(),
        MetricsUtil.name(getClass(), "keyspaceNotificationExecutor"),
        MetricsUtil.PREFIX);
    ExecutorService apnSenderExecutor = environment.lifecycle().executorService(name(getClass(), "apnSender-%d"))
        .maxThreads(1).minThreads(1).build();
    ExecutorService fcmSenderExecutor = environment.lifecycle().executorService(name(getClass(), "fcmSender-%d"))
        .maxThreads(32).minThreads(32).workQueue(fcmSenderQueue).build();
    ExecutorService secureValueRecoveryServiceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "secureValueRecoveryService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService storageServiceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "storageService-%d")).maxThreads(1).minThreads(1).build();
    ScheduledExecutorService secureValueRecoveryServiceRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "secureValueRecoveryServiceRetry-%d")).threads(1).build();
    ScheduledExecutorService storageServiceRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "storageServiceRetry-%d")).threads(1).build();
    ScheduledExecutorService hcaptchaRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "hCaptchaRetry-%d")).threads(1).build();
    ScheduledExecutorService remoteStorageExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "remoteStorageRetry-%d")).threads(1).build();
    ScheduledExecutorService registrationIdentityTokenRefreshExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "registrationIdentityTokenRefresh-%d")).threads(1).build();

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
    ExecutorService accountLockExecutor = environment.lifecycle()
        .executorService(name(getClass(), "accountLock-%d"))
        .minThreads(8)
        .maxThreads(8)
        .build();
    ExecutorService clientPresenceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "clientPresence-%d"))
        .minThreads(8)
        .maxThreads(8)
        .build();
    ScheduledExecutorService subscriptionProcessorRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "subscriptionProcessorRetry-%d")).threads(1).build();

    StripeManager stripeManager = new StripeManager(config.getStripe().apiKey().value(), subscriptionProcessorExecutor,
        config.getStripe().idempotencyKeyGenerator().value(), config.getStripe().boostDescription(), config.getStripe().supportedCurrenciesByPaymentMethod());
    BraintreeManager braintreeManager = new BraintreeManager(config.getBraintree().merchantId(),
        config.getBraintree().publicKey(), config.getBraintree().privateKey().value(),
        config.getBraintree().environment(),
        config.getBraintree().supportedCurrenciesByPaymentMethod(), config.getBraintree().merchantAccounts(),
        config.getBraintree().graphqlUrl(), config.getBraintree().circuitBreaker(), subscriptionProcessorExecutor,
        subscriptionProcessorRetryExecutor);

    ExternalServiceCredentialsGenerator directoryV2CredentialsGenerator = DirectoryV2Controller.credentialsGenerator(
        config.getDirectoryV2Configuration().getDirectoryV2ClientConfiguration());
    ExternalServiceCredentialsGenerator storageCredentialsGenerator = SecureStorageController.credentialsGenerator(
        config.getSecureStorageServiceConfiguration());
    ExternalServiceCredentialsGenerator paymentsCredentialsGenerator = PaymentsController.credentialsGenerator(
        config.getPaymentsServiceConfiguration());
    ExternalServiceCredentialsGenerator artCredentialsGenerator = ArtController.credentialsGenerator(
        config.getArtServiceConfiguration());
    ExternalServiceCredentialsGenerator svr2CredentialsGenerator = SecureValueRecovery2Controller.credentialsGenerator(
            config.getSvr2Configuration());
    ExternalServiceCredentialsGenerator svr3CredentialsGenerator = SecureValueRecovery3Controller.credentialsGenerator(
        config.getSvr3Configuration());

    dynamicConfigurationManager.start();

    ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(
        dynamicConfigurationManager);
    RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = new RegistrationRecoveryPasswordsManager(
        registrationRecoveryPasswords);
    UsernameHashZkProofVerifier usernameHashZkProofVerifier = new UsernameHashZkProofVerifier();

    RegistrationServiceClient registrationServiceClient = new RegistrationServiceClient(
        config.getRegistrationServiceConfiguration().host(),
        config.getRegistrationServiceConfiguration().port(),
        config.getRegistrationServiceConfiguration().credentialConfigurationJson(),
        config.getRegistrationServiceConfiguration().identityTokenAudience(),
        config.getRegistrationServiceConfiguration().registrationCaCertificate(),
        registrationCallbackExecutor,
        registrationIdentityTokenRefreshExecutor);
    SecureValueRecovery2Client secureValueRecovery2Client = new SecureValueRecovery2Client(svr2CredentialsGenerator,
        secureValueRecoveryServiceExecutor, secureValueRecoveryServiceRetryExecutor, config.getSvr2Configuration());
    SecureStorageClient secureStorageClient = new SecureStorageClient(storageCredentialsGenerator,
        storageServiceExecutor, storageServiceRetryExecutor, config.getSecureStorageServiceConfiguration());
    ClientPresenceManager clientPresenceManager = new ClientPresenceManager(clientPresenceCluster, recurringJobExecutor,
        keyspaceNotificationDispatchExecutor);
    ProfilesManager profilesManager = new ProfilesManager(profiles, cacheCluster);
    MessagesCache messagesCache = new MessagesCache(messagesCluster, messagesCluster,
        keyspaceNotificationDispatchExecutor, messageDeliveryScheduler, messageDeletionAsyncExecutor, clock);
    ClientReleaseManager clientReleaseManager = new ClientReleaseManager(clientReleases,
        recurringJobExecutor,
        config.getClientReleaseConfiguration().refreshInterval(),
        Clock.systemUTC());
    PushLatencyManager pushLatencyManager = new PushLatencyManager(metricsCluster, clientReleaseManager);
    ReportMessageManager reportMessageManager = new ReportMessageManager(reportMessageDynamoDb, rateLimitersCluster,
        config.getReportMessageConfiguration().getCounterTtl());
    MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, reportMessageManager,
        messageDeletionAsyncExecutor);
    AccountLockManager accountLockManager = new AccountLockManager(dynamoDbClient,
        config.getDynamoDbTables().getDeletedAccountsLock().getTableName());
    ClientPublicKeysManager clientPublicKeysManager = new ClientPublicKeysManager(clientPublicKeys);
    AccountsManager accountsManager = new AccountsManager(accounts, phoneNumberIdentifiers, cacheCluster,
        accountLockManager, keysManager, messagesManager, profilesManager,
        secureStorageClient, secureValueRecovery2Client,
        clientPresenceManager,
        experimentEnrollmentManager, registrationRecoveryPasswordsManager, accountLockExecutor, clientPresenceExecutor,
        clock);
    RemoteConfigsManager remoteConfigsManager = new RemoteConfigsManager(remoteConfigs);
    APNSender apnSender = new APNSender(apnSenderExecutor, config.getApnConfiguration());
    FcmSender fcmSender = new FcmSender(fcmSenderExecutor, config.getFcmConfiguration().credentials().value());
    ApnPushNotificationScheduler apnPushNotificationScheduler = new ApnPushNotificationScheduler(pushSchedulerCluster,
        apnSender, accountsManager, 0);
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
    OneTimeDonationsManager oneTimeDonationsManager = new OneTimeDonationsManager(
        config.getDynamoDbTables().getOnetimeDonations().getTableName(), config.getDynamoDbTables().getOnetimeDonations().getExpiration(), dynamoDbAsyncClient);
    RedeemedReceiptsManager redeemedReceiptsManager = new RedeemedReceiptsManager(clock,
        config.getDynamoDbTables().getRedeemedReceipts().getTableName(),
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getRedeemedReceipts().getExpiration());
    SubscriptionManager subscriptionManager = new SubscriptionManager(
        config.getDynamoDbTables().getSubscriptions().getTableName(), dynamoDbAsyncClient);

    final RegistrationLockVerificationManager registrationLockVerificationManager = new RegistrationLockVerificationManager(
        accountsManager, clientPresenceManager, svr2CredentialsGenerator, registrationRecoveryPasswordsManager, pushNotificationManager, rateLimiters);
    final PhoneVerificationTokenManager phoneVerificationTokenManager = new PhoneVerificationTokenManager(
        registrationServiceClient, registrationRecoveryPasswordsManager);

    final ReportedMessageMetricsListener reportedMessageMetricsListener = new ReportedMessageMetricsListener(
        accountsManager);
    reportMessageManager.addListener(reportedMessageMetricsListener);

    final AccountAuthenticator accountAuthenticator = new AccountAuthenticator(accountsManager);

    final MessageSender messageSender = new MessageSender(clientPresenceManager, messagesManager,
        pushNotificationManager,
        pushLatencyManager);
    final ReceiptSender receiptSender = new ReceiptSender(accountsManager, messageSender, receiptSenderExecutor);
    final TurnTokenGenerator turnTokenGenerator = new TurnTokenGenerator(dynamicConfigurationManager,
        config.getTurnSecretConfiguration().secret().value());

    final CardinalityEstimator messageByteLimitCardinalityEstimator = new CardinalityEstimator(
        rateLimitersCluster,
        "message_byte_limit",
        config.getMessageByteLimitCardinalityEstimator().period());

    RecaptchaClient recaptchaClient = new RecaptchaClient(
        config.getRecaptchaConfiguration().projectPath(),
        config.getRecaptchaConfiguration().credentialConfigurationJson(),
        dynamicConfigurationManager);
    HCaptchaClient hCaptchaClient = new HCaptchaClient(
        config.getHCaptchaConfiguration().getApiKey().value(),
        hcaptchaRetryExecutor,
        config.getHCaptchaConfiguration().getCircuitBreaker(),
        config.getHCaptchaConfiguration().getRetry(),
        dynamicConfigurationManager);
    HttpClient shortCodeRetrieverHttpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(10)).build();
    ShortCodeExpander shortCodeRetriever = new ShortCodeExpander(shortCodeRetrieverHttpClient, config.getShortCodeRetrieverConfiguration().baseUrl());
    CaptchaChecker captchaChecker = new CaptchaChecker(shortCodeRetriever, List.of(recaptchaClient, hCaptchaClient));

    PushChallengeManager pushChallengeManager = new PushChallengeManager(pushNotificationManager,
        pushChallengeDynamoDb);

    ChangeNumberManager changeNumberManager = new ChangeNumberManager(messageSender, accountsManager);

    HttpClient currencyClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(10)).build();
    FixerClient fixerClient = new FixerClient(currencyClient, config.getPaymentsServiceConfiguration().fixerApiKey().value());
    CoinMarketCapClient coinMarketCapClient = new CoinMarketCapClient(currencyClient, config.getPaymentsServiceConfiguration().coinMarketCapApiKey().value(), config.getPaymentsServiceConfiguration().coinMarketCapCurrencyIds());
    CurrencyConversionManager currencyManager = new CurrencyConversionManager(fixerClient, coinMarketCapClient,
        cacheCluster, config.getPaymentsServiceConfiguration().paymentCurrencies(), recurringJobExecutor, Clock.systemUTC());

    environment.lifecycle().manage(apnSender);
    environment.lifecycle().manage(apnPushNotificationScheduler);
    environment.lifecycle().manage(provisioningManager);
    environment.lifecycle().manage(messagesCache);
    environment.lifecycle().manage(clientPresenceManager);
    environment.lifecycle().manage(currencyManager);
    environment.lifecycle().manage(registrationServiceClient);
    environment.lifecycle().manage(clientReleaseManager);

    final RegistrationCaptchaManager registrationCaptchaManager = new RegistrationCaptchaManager(captchaChecker,
        rateLimiters, config.getTestDevices(), dynamicConfigurationManager);

    StaticCredentialsProvider cdnCredentialsProvider = StaticCredentialsProvider
        .create(AwsBasicCredentials.create(
            config.getCdnConfiguration().accessKey().value(),
            config.getCdnConfiguration().accessSecret().value()));
    S3Client cdnS3Client = S3Client.builder()
        .credentialsProvider(cdnCredentialsProvider)
        .region(Region.of(config.getCdnConfiguration().region()))
        .httpClientBuilder(AwsCrtHttpClient.builder())
        .build();
    S3AsyncClient asyncCdnS3Client = S3AsyncClient.builder()
        .credentialsProvider(cdnCredentialsProvider)
        .region(Region.of(config.getCdnConfiguration().region()))
        .build();

    final GcsAttachmentGenerator gcsAttachmentGenerator = new GcsAttachmentGenerator(
        config.getGcpAttachmentsConfiguration().domain(),
        config.getGcpAttachmentsConfiguration().email(),
        config.getGcpAttachmentsConfiguration().maxSizeInBytes(),
        config.getGcpAttachmentsConfiguration().pathPrefix(),
        config.getGcpAttachmentsConfiguration().rsaSigningKey().value());

    PostPolicyGenerator profileCdnPolicyGenerator = new PostPolicyGenerator(config.getCdnConfiguration().region(),
        config.getCdnConfiguration().bucket(), config.getCdnConfiguration().accessKey().value());
    PolicySigner profileCdnPolicySigner = new PolicySigner(config.getCdnConfiguration().accessSecret().value(),
        config.getCdnConfiguration().region());

    ServerSecretParams zkSecretParams = new ServerSecretParams(config.getZkConfig().serverSecret().value());
    GenericServerSecretParams callingGenericZkSecretParams = new GenericServerSecretParams(config.getCallingZkConfig().serverSecret().value());
    GenericServerSecretParams backupsGenericZkSecretParams = new GenericServerSecretParams(config.getBackupsZkConfig().serverSecret().value());
    ServerZkProfileOperations zkProfileOperations = new ServerZkProfileOperations(zkSecretParams);
    ServerZkAuthOperations zkAuthOperations = new ServerZkAuthOperations(zkSecretParams);
    ServerZkReceiptOperations zkReceiptOperations = new ServerZkReceiptOperations(zkSecretParams);

    Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator = new Cdn3BackupCredentialGenerator(config.getTus());
    BackupAuthManager backupAuthManager = new BackupAuthManager(dynamicConfigurationManager, rateLimiters, accountsManager, backupsGenericZkSecretParams, clock);
    BackupsDb backupsDb = new BackupsDb(
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getBackups().getTableName(),
        clock);
    BackupManager backupManager = new BackupManager(
        backupsDb,
        backupsGenericZkSecretParams,
        cdn3BackupCredentialGenerator,
        new Cdn3RemoteStorageManager(
            remoteStorageExecutor,
            config.getClientCdnConfiguration().getCircuitBreaker(),
            config.getClientCdnConfiguration().getRetry(),
            config.getClientCdnConfiguration().getCaCertificates(),
            config.getCdn3StorageManagerConfiguration()),
        config.getClientCdnConfiguration().getAttachmentUrls(),
        clock);

    final BasicCredentialAuthenticationInterceptor basicCredentialAuthenticationInterceptor =
        new BasicCredentialAuthenticationInterceptor(new AccountAuthenticator(accountsManager));

    final ServerBuilder<?> grpcServer = ServerBuilder.forPort(config.getGrpcPort())
        .addService(ServerInterceptors.intercept(new AccountsGrpcService(accountsManager, rateLimiters, usernameHashZkProofVerifier, registrationRecoveryPasswordsManager), basicCredentialAuthenticationInterceptor))
        .addService(new AccountsAnonymousGrpcService(accountsManager, rateLimiters))
        .addService(ExternalServiceCredentialsGrpcService.createForAllExternalServices(config, rateLimiters))
        .addService(ExternalServiceCredentialsAnonymousGrpcService.create(accountsManager, config))
        .addService(ServerInterceptors.intercept(new KeysGrpcService(accountsManager, keysManager, rateLimiters), basicCredentialAuthenticationInterceptor))
        .addService(new KeysAnonymousGrpcService(accountsManager, keysManager))
        .addService(new PaymentsGrpcService(currencyManager))
        .addService(ServerInterceptors.intercept(new ProfileGrpcService(clock, accountsManager, profilesManager, dynamicConfigurationManager,
                config.getBadges(), asyncCdnS3Client, profileCdnPolicyGenerator, profileCdnPolicySigner, profileBadgeConverter, rateLimiters, zkProfileOperations, config.getCdnConfiguration().bucket()), basicCredentialAuthenticationInterceptor))
        .addService(new ProfileAnonymousGrpcService(accountsManager, profilesManager, profileBadgeConverter, zkProfileOperations));

    RemoteDeprecationFilter remoteDeprecationFilter = new RemoteDeprecationFilter(dynamicConfigurationManager);
    environment.servlets()
        .addFilter("RemoteDeprecationFilter", remoteDeprecationFilter)
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");

    // Note: interceptors run in the reverse order they are added; the remote deprecation filter
    // depends on the user-agent context so it has to come first here!
    // http://grpc.github.io/grpc-java/javadoc/io/grpc/ServerBuilder.html#intercept-io.grpc.ServerInterceptor-
    grpcServer
        // TODO: specialize metrics with user-agent platform
        .intercept(new MetricCollectingServerInterceptor(Metrics.globalRegistry))
        .intercept(new ErrorMappingInterceptor())
        .intercept(new AcceptLanguageInterceptor())
        .intercept(remoteDeprecationFilter)
        .intercept(new UserAgentInterceptor());

    environment.lifecycle().manage(new GrpcServerManagedWrapper(grpcServer.build()));

    final AuthFilter<BasicCredentials, AuthenticatedAccount> accountAuthFilter =
        new BasicCredentialAuthFilter.Builder<AuthenticatedAccount>()
            .setAuthenticator(accountAuthenticator)
            .buildAuthFilter();

    environment.jersey().register(new RequestStatisticsFilter(TrafficSource.HTTP));
    environment.jersey().register(MultiRecipientMessageProvider.class);
    environment.jersey().register(new MetricsApplicationEventListener(TrafficSource.HTTP, clientReleaseManager));
    environment.jersey().register(new AuthDynamicFeature(accountAuthFilter));
    environment.jersey().register(new AuthValueFactoryProvider.Binder<>(AuthenticatedAccount.class));
    environment.jersey().register(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager));
    environment.jersey().register(new TimestampResponseFilter());

    ///
    WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment = new WebSocketEnvironment<>(environment,
        config.getWebSocketConfiguration(), Duration.ofMillis(90000));
    webSocketEnvironment.setAuthenticator(new WebSocketAccountAuthenticator(accountAuthenticator));
    webSocketEnvironment.setConnectListener(
        new AuthenticatedConnectListener(receiptSender, messagesManager, pushNotificationManager,
            clientPresenceManager, websocketScheduledExecutor, messageDeliveryScheduler, clientReleaseManager));
    webSocketEnvironment.jersey()
        .register(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager));
    webSocketEnvironment.jersey().register(new RequestStatisticsFilter(TrafficSource.WEBSOCKET));
    webSocketEnvironment.jersey().register(MultiRecipientMessageProvider.class);
    webSocketEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET, clientReleaseManager));
    webSocketEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));

    boolean registeredSpamFilter = false;
    ReportSpamTokenProvider reportSpamTokenProvider = null;

    List<RateLimitChallengeListener> rateLimitChallengeListeners = new ArrayList<>();
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
        rateLimitChallengeListeners.add((RateLimitChallengeListener) filter);
      }
    }
    RateLimitChallengeManager rateLimitChallengeManager = new RateLimitChallengeManager(pushChallengeManager,
        captchaChecker, rateLimiters, rateLimitChallengeListeners);


    if (!registeredSpamFilter) {
      log.warn("No spam filters installed");
    }

    if (reportSpamTokenProvider == null) {
      log.warn("No spam-reporting token providers found; using default (no-op) provider as a default");
      reportSpamTokenProvider = ReportSpamTokenProvider.noop();
    }

    final List<Object> commonControllers = Lists.newArrayList(
        new AccountController(accountsManager, rateLimiters, turnTokenGenerator, registrationRecoveryPasswordsManager,
            usernameHashZkProofVerifier),
        new AccountControllerV2(accountsManager, changeNumberManager, phoneVerificationTokenManager,
            registrationLockVerificationManager, rateLimiters),
        new ArtController(rateLimiters, artCredentialsGenerator),
        new AttachmentControllerV2(rateLimiters, config.getAwsAttachmentsConfiguration().accessKey().value(),
            config.getAwsAttachmentsConfiguration().accessSecret().value(),
            config.getAwsAttachmentsConfiguration().region(), config.getAwsAttachmentsConfiguration().bucket()),
        new AttachmentControllerV3(rateLimiters, gcsAttachmentGenerator),
        new AttachmentControllerV4(rateLimiters, gcsAttachmentGenerator, new TusAttachmentGenerator(config.getTus()),
            experimentEnrollmentManager),
        new ArchiveController(backupAuthManager, backupManager),
        new CallLinkController(rateLimiters, callingGenericZkSecretParams),
        new CertificateController(new CertificateGenerator(config.getDeliveryCertificate().certificate().value(),
            config.getDeliveryCertificate().ecPrivateKey(), config.getDeliveryCertificate().expiresDays()),
            zkAuthOperations, callingGenericZkSecretParams, clock),
        new ChallengeController(rateLimitChallengeManager, useRemoteAddress),
        new DeviceController(config.getLinkDeviceSecretConfiguration().secret().value(), accountsManager,
            rateLimiters, rateLimitersCluster, config.getMaxDevices(), clock),
        new DirectoryV2Controller(directoryV2CredentialsGenerator),
        new DonationController(clock, zkReceiptOperations, redeemedReceiptsManager, accountsManager, config.getBadges(),
            ReceiptCredentialPresentation::new),
        new KeysController(rateLimiters, keysManager, accountsManager),
        new MessageController(rateLimiters, messageByteLimitCardinalityEstimator, messageSender, receiptSender,
            accountsManager, messagesManager, pushNotificationManager, reportMessageManager,
            multiRecipientMessageExecutor, messageDeliveryScheduler, reportSpamTokenProvider, clientReleaseManager,
            dynamicConfigurationManager, zkSecretParams),
        new PaymentsController(currencyManager, paymentsCredentialsGenerator),
        new ProfileController(clock, rateLimiters, accountsManager, profilesManager, dynamicConfigurationManager,
            profileBadgeConverter, config.getBadges(), cdnS3Client, profileCdnPolicyGenerator, profileCdnPolicySigner,
            config.getCdnConfiguration().bucket(), zkProfileOperations, batchIdentityCheckExecutor),
        new ProvisioningController(rateLimiters, provisioningManager),
        new RegistrationController(accountsManager, phoneVerificationTokenManager, registrationLockVerificationManager,
            rateLimiters),
        new RemoteConfigController(remoteConfigsManager, config.getRemoteConfigConfiguration().globalConfig(), clock),
        new SecureStorageController(storageCredentialsGenerator),
        new SecureValueRecovery2Controller(svr2CredentialsGenerator, accountsManager),
        new SecureValueRecovery3Controller(svr3CredentialsGenerator, accountsManager),
        new StickerController(rateLimiters, config.getCdnConfiguration().accessKey().value(),
            config.getCdnConfiguration().accessSecret().value(), config.getCdnConfiguration().region(),
            config.getCdnConfiguration().bucket()),
        new VerificationController(registrationServiceClient, new VerificationSessionManager(verificationSessions),
            pushNotificationManager, registrationCaptchaManager, registrationRecoveryPasswordsManager, rateLimiters,
            accountsManager, useRemoteAddress, dynamicConfigurationManager, clock)
    );
    if (config.getSubscription() != null && config.getOneTimeDonations() != null) {
      commonControllers.add(new SubscriptionController(clock, config.getSubscription(), config.getOneTimeDonations(),
          subscriptionManager, stripeManager, braintreeManager, zkReceiptOperations, issuedReceiptsManager, oneTimeDonationsManager,
          profileBadgeConverter, resourceBundleLevelTranslator, bankMandateTranslator));
    }

    for (Object controller : commonControllers) {
      environment.jersey().register(controller);
      webSocketEnvironment.jersey().register(controller);
    }

    WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment = new WebSocketEnvironment<>(environment,
        webSocketEnvironment.getRequestLog(), Duration.ofMillis(60000));
    provisioningEnvironment.jersey().register(new WebsocketRefreshApplicationEventListener(accountsManager, clientPresenceManager));
    provisioningEnvironment.setConnectListener(new ProvisioningConnectListener(provisioningManager));
    provisioningEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET, clientReleaseManager));
    provisioningEnvironment.jersey().register(new KeepAliveController(clientPresenceManager));

    registerCorsFilter(environment);
    registerExceptionMappers(environment, webSocketEnvironment, provisioningEnvironment);
    registerProviders(environment, webSocketEnvironment, provisioningEnvironment);

    environment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
    webSocketEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
    provisioningEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);

    JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), null);

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

    MetricsUtil.registerSystemResourceMetrics(environment);
  }


  private void registerProviders(Environment environment,
      WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment,
      WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment) {
    List.of(
            ScoreThresholdProvider.ScoreThresholdFeature.class,
            SenderOverrideProvider.SenderOverrideFeature.class,
            PushChallengeConfigProvider.PushChallengeConfigFeature.class)
    .forEach(feature -> {
          environment.jersey().register(feature);
          webSocketEnvironment.jersey().register(feature);
          provisioningEnvironment.jersey().register(feature);
    });
  }

  private void registerExceptionMappers(Environment environment,
      WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment,
      WebSocketEnvironment<AuthenticatedAccount> provisioningEnvironment) {

    List.of(
        new LoggingUnhandledExceptionMapper(),
        new CompletionExceptionMapper(),
        new GrpcStatusRuntimeExceptionMapper(),
        new IOExceptionMapper(),
        new RateLimitExceededExceptionMapper(),
        new InvalidWebsocketAddressExceptionMapper(),
        new DeviceLimitExceededExceptionMapper(),
        new ServerRejectedExceptionMapper(),
        new ImpossiblePhoneNumberExceptionMapper(),
        new NonNormalizedPhoneNumberExceptionMapper(),
        new RegistrationServiceSenderExceptionMapper(),
        new SubscriptionProcessorExceptionMapper(),
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
