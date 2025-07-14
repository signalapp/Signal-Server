/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.Lists;
import com.webauthn4j.appattest.DeviceCheckManager;
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
import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.grpc.MetricCollectingServerInterceptor;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.ServletRegistration;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
import org.whispersystems.textsecuregcm.attachments.DummyAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.CloudflareTurnCredentialsManager;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.grpc.ProhibitAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.auth.grpc.RequireAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupsDb;
import org.whispersystems.textsecuregcm.backup.Cdn3BackupCredentialGenerator;
import org.whispersystems.textsecuregcm.backup.Cdn3RemoteStorageManager;
import org.whispersystems.textsecuregcm.badges.ConfiguredProfileBadgeConverter;
import org.whispersystems.textsecuregcm.captcha.CaptchaChecker;
import org.whispersystems.textsecuregcm.captcha.CaptchaClient;
import org.whispersystems.textsecuregcm.captcha.RegistrationCaptchaManager;
import org.whispersystems.textsecuregcm.captcha.ShortCodeExpander;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretStore;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretsModule;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.AccountControllerV2;
import org.whispersystems.textsecuregcm.controllers.ArchiveController;
import org.whispersystems.textsecuregcm.controllers.AttachmentControllerV4;
import org.whispersystems.textsecuregcm.controllers.CallLinkController;
import org.whispersystems.textsecuregcm.controllers.CallRoutingControllerV2;
import org.whispersystems.textsecuregcm.controllers.CertificateController;
import org.whispersystems.textsecuregcm.controllers.ChallengeController;
import org.whispersystems.textsecuregcm.controllers.DeviceCheckController;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.controllers.DirectoryV2Controller;
import org.whispersystems.textsecuregcm.controllers.DonationController;
import org.whispersystems.textsecuregcm.controllers.KeepAliveController;
import org.whispersystems.textsecuregcm.controllers.KeyTransparencyController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.OneTimeDonationController;
import org.whispersystems.textsecuregcm.controllers.PaymentsController;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.ProvisioningController;
import org.whispersystems.textsecuregcm.controllers.RegistrationController;
import org.whispersystems.textsecuregcm.controllers.RemoteConfigController;
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.controllers.SecureValueRecovery2Controller;
import org.whispersystems.textsecuregcm.controllers.StickerController;
import org.whispersystems.textsecuregcm.controllers.SubscriptionController;
import org.whispersystems.textsecuregcm.controllers.VerificationController;
import org.whispersystems.textsecuregcm.currency.CoinGeckoClient;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.currency.FixerClient;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.filters.ExternalRequestFilter;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.filters.RemoteDeprecationFilter;
import org.whispersystems.textsecuregcm.filters.RequestStatisticsFilter;
import org.whispersystems.textsecuregcm.filters.RestDeprecationFilter;
import org.whispersystems.textsecuregcm.filters.TimestampResponseFilter;
import org.whispersystems.textsecuregcm.grpc.AccountsAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.AccountsGrpcService;
import org.whispersystems.textsecuregcm.grpc.ErrorMappingInterceptor;
import org.whispersystems.textsecuregcm.grpc.ExternalServiceCredentialsAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.ExternalServiceCredentialsGrpcService;
import org.whispersystems.textsecuregcm.grpc.KeysAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.KeysGrpcService;
import org.whispersystems.textsecuregcm.grpc.PaymentsGrpcService;
import org.whispersystems.textsecuregcm.grpc.ProfileAnonymousGrpcService;
import org.whispersystems.textsecuregcm.grpc.ProfileGrpcService;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesInterceptor;
import org.whispersystems.textsecuregcm.grpc.ValidatingInterceptor;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.grpc.net.ManagedDefaultEventLoopGroup;
import org.whispersystems.textsecuregcm.grpc.net.ManagedLocalGrpcServer;
import org.whispersystems.textsecuregcm.grpc.net.ManagedNioEventLoopGroup;
import org.whispersystems.textsecuregcm.grpc.net.noisedirect.NoiseDirectTunnelServer;
import org.whispersystems.textsecuregcm.grpc.net.websocket.NoiseWebSocketTunnelServer;
import org.whispersystems.textsecuregcm.jetty.JettyHttpConfigurationCustomizer;
import org.whispersystems.textsecuregcm.keytransparency.KeyTransparencyServiceClient;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.limits.NoopMessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.limits.PushChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimitByIpFilter;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.limits.RedisMessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.GrpcStatusRuntimeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.IOExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.InvalidWebsocketAddressExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.JsonMappingExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ObsoletePhoneNumberFormatExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RegistrationServiceSenderExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ServerRejectedExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.SubscriptionExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsApplicationEventListener;
import org.whispersystems.textsecuregcm.metrics.MetricsHttpChannelListener;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.MicrometerAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.metrics.ReportedMessageMetricsListener;
import org.whispersystems.textsecuregcm.metrics.TlsCertificateExpirationUtil;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.providers.RedisClusterHealthCheck;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.DummySender;
import org.whispersystems.textsecuregcm.push.FcmSender;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.push.WebSocketConnectionEventManager;
import org.whispersystems.textsecuregcm.redis.ConnectionEventLogger;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.securestorage.InsecureStorageClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securestorage.StorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.InsecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.securevaluerecovery.ValueRecovery2Client;
import org.whispersystems.textsecuregcm.spam.ChallengeConstraintChecker;
import org.whispersystems.textsecuregcm.spam.RegistrationFraudChecker;
import org.whispersystems.textsecuregcm.spam.RegistrationRecoveryChecker;
import org.whispersystems.textsecuregcm.spam.SpamChecker;
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
import org.whispersystems.textsecuregcm.storage.PagedSingleUseKEMPreKeyStore;
import org.whispersystems.textsecuregcm.storage.PersistentTimer;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.PushChallengeDynamoDb;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.storage.RepeatedUseECSignedPreKeyStore;
import org.whispersystems.textsecuregcm.storage.RepeatedUseKEMSignedPreKeyStore;
import org.whispersystems.textsecuregcm.storage.ReportMessageDynamoDb;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.SingleUseECPreKeyStore;
import org.whispersystems.textsecuregcm.storage.SingleUseKEMPreKeyStore;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.Subscriptions;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessions;
import org.whispersystems.textsecuregcm.storage.devicecheck.AppleDeviceCheckManager;
import org.whispersystems.textsecuregcm.storage.devicecheck.AppleDeviceCheckTrustAnchor;
import org.whispersystems.textsecuregcm.storage.devicecheck.AppleDeviceChecks;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.BankMandateTranslator;
import org.whispersystems.textsecuregcm.subscriptions.BraintreeManager;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.subscriptions.StripeManager;
import org.whispersystems.textsecuregcm.util.BufferingInterceptor;
import org.whispersystems.textsecuregcm.util.ManagedAwsCrt;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;
import org.whispersystems.textsecuregcm.util.VirtualExecutorServiceProvider;
import org.whispersystems.textsecuregcm.util.VirtualThreadPinEventMonitor;
import org.whispersystems.textsecuregcm.util.logging.LoggingUnhandledExceptionMapper;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;
import org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.textsecuregcm.workers.BackupMetricsCommand;
import org.whispersystems.textsecuregcm.workers.BackupUsageRecalculationCommand;
import org.whispersystems.textsecuregcm.workers.CertificateCommand;
import org.whispersystems.textsecuregcm.workers.CheckDynamicConfigurationCommand;
import org.whispersystems.textsecuregcm.workers.DeleteUserCommand;
import org.whispersystems.textsecuregcm.workers.IdleDeviceNotificationSchedulerFactory;
import org.whispersystems.textsecuregcm.workers.MessagePersisterServiceCommand;
import org.whispersystems.textsecuregcm.workers.NotifyIdleDevicesCommand;
import org.whispersystems.textsecuregcm.workers.ProcessScheduledJobsServiceCommand;
import org.whispersystems.textsecuregcm.workers.RegenerateSecondaryDynamoDbTableDataCommand;
import org.whispersystems.textsecuregcm.workers.RemoveExpiredAccountsCommand;
import org.whispersystems.textsecuregcm.workers.RemoveExpiredBackupsCommand;
import org.whispersystems.textsecuregcm.workers.RemoveExpiredLinkedDevicesCommand;
import org.whispersystems.textsecuregcm.workers.RemoveExpiredUsernameHoldsCommand;
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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class WhisperServerService extends Application<WhisperServerConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(WhisperServerService.class);

  public static final String SECRETS_BUNDLE_FILE_NAME_PROPERTY = "secrets.bundle.filename";

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
    bootstrap.addCommand(new UnlinkDeviceCommand());
    bootstrap.addCommand(new ScheduledApnPushNotificationSenderServiceCommand());
    bootstrap.addCommand(new MessagePersisterServiceCommand());
    bootstrap.addCommand(new RemoveExpiredAccountsCommand(Clock.systemUTC()));
    bootstrap.addCommand(new RemoveExpiredUsernameHoldsCommand(Clock.systemUTC()));
    bootstrap.addCommand(new RemoveExpiredBackupsCommand(Clock.systemUTC()));
    bootstrap.addCommand(new BackupMetricsCommand(Clock.systemUTC()));
    bootstrap.addCommand(new BackupUsageRecalculationCommand());
    bootstrap.addCommand(new RemoveExpiredLinkedDevicesCommand());
    bootstrap.addCommand(new NotifyIdleDevicesCommand());

    bootstrap.addCommand(new ProcessScheduledJobsServiceCommand("process-idle-device-notification-jobs",
        "Processes scheduled jobs to send notifications to idle devices",
        new IdleDeviceNotificationSchedulerFactory()));

    bootstrap.addCommand(new RegenerateSecondaryDynamoDbTableDataCommand());
  }

  @Override
  public String getName() {
    return "whisper-server";
  }

  @Override
  public void run(WhisperServerConfiguration config, Environment environment) throws Exception {
    final Clock clock = Clock.systemUTC();
    final int availableProcessors = Runtime.getRuntime().availableProcessors();

    final AwsCredentialsProvider awsCredentialsProvider = config.getAwsCredentialsConfiguration().build();

    UncaughtExceptionHandler.register();

    ScheduledExecutorService dynamicConfigurationExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "dynamicConfiguration-%d")).threads(1).build();

    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new DynamicConfigurationManager<>(
            config.getDynamicConfig().build(awsCredentialsProvider, dynamicConfigurationExecutor), DynamicConfiguration.class);
    dynamicConfigurationManager.start();

    MetricsUtil.configureRegistries(config, environment, dynamicConfigurationManager);

    if (config.getServerFactory() instanceof DefaultServerFactory defaultServerFactory) {
      defaultServerFactory.getApplicationConnectors()
          .forEach(connectorFactory -> {
            if (connectorFactory instanceof HttpsConnectorFactory h) {
              h.setKeyStorePassword(config.getTlsKeyStoreConfiguration().password().value());

              TlsCertificateExpirationUtil.configureMetrics(h.getKeyStorePath(), h.getKeyStorePassword(), h.getKeyStoreType(), h.getKeyStoreProvider());
            }
          });
    }

    environment.lifecycle().addEventListener(new JettyHttpConfigurationCustomizer());

    HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup =
        new HeaderControlledResourceBundleLookup();
    ConfiguredProfileBadgeConverter profileBadgeConverter = new ConfiguredProfileBadgeConverter(
        clock, config.getBadges(), headerControlledResourceBundleLookup);
    BankMandateTranslator bankMandateTranslator = new BankMandateTranslator(headerControlledResourceBundleLookup);

    environment.lifecycle().manage(new ManagedAwsCrt());

    final ExecutorService awsSdkMetricsExecutor = environment.lifecycle()
        .virtualExecutorService(name(getClass(), "awsSdkMetrics-%d"));

    final DynamoDbAsyncClient dynamoDbAsyncClient = config.getDynamoDbClientConfiguration()
        .buildAsyncClient(awsCredentialsProvider, new MicrometerAwsSdkMetricPublisher(awsSdkMetricsExecutor, "dynamoDbAsync"));

    final DynamoDbClient dynamoDbClient = config.getDynamoDbClientConfiguration()
        .buildSyncClient(awsCredentialsProvider, new MicrometerAwsSdkMetricPublisher(awsSdkMetricsExecutor, "dynamoDbSync"));

    final AwsCredentialsProvider cdnCredentialsProvider = config.getCdnConfiguration().credentials().build();
    final S3AsyncClient asyncCdnS3Client = S3AsyncClient.builder()
        .credentialsProvider(cdnCredentialsProvider)
        .region(Region.of(config.getCdnConfiguration().region()))
        .build();

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
        clock,
        dynamoDbClient,
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getAccounts().getTableName(),
        config.getDynamoDbTables().getAccounts().getPhoneNumberTableName(),
        config.getDynamoDbTables().getAccounts().getPhoneNumberIdentifierTableName(),
        config.getDynamoDbTables().getAccounts().getUsernamesTableName(),
        config.getDynamoDbTables().getDeletedAccounts().getTableName(),
        config.getDynamoDbTables().getAccounts().getUsedLinkDeviceTokensTableName());
    ClientReleases clientReleases = new ClientReleases(dynamoDbAsyncClient,
        config.getDynamoDbTables().getClientReleases().getTableName());
    PhoneNumberIdentifiers phoneNumberIdentifiers = new PhoneNumberIdentifiers(dynamoDbAsyncClient,
        config.getDynamoDbTables().getPhoneNumberIdentifiers().getTableName());
    Profiles profiles = new Profiles(dynamoDbClient, dynamoDbAsyncClient,
        config.getDynamoDbTables().getProfiles().getTableName());

    S3AsyncClient asyncKeysS3Client = S3AsyncClient.builder()
        .credentialsProvider(awsCredentialsProvider)
        .region(Region.of(config.getPagedSingleUseKEMPreKeyStore().region()))
        .build();
    KeysManager keysManager = new KeysManager(
        new SingleUseECPreKeyStore(dynamoDbAsyncClient, config.getDynamoDbTables().getEcKeys().getTableName()),
        new SingleUseKEMPreKeyStore(dynamoDbAsyncClient, config.getDynamoDbTables().getKemKeys().getTableName()),
        new PagedSingleUseKEMPreKeyStore(
            dynamoDbAsyncClient,
            asyncKeysS3Client,
            config.getDynamoDbTables().getPagedKemKeys().getTableName(),
            config.getPagedSingleUseKEMPreKeyStore().bucket()),
        new RepeatedUseECSignedPreKeyStore(dynamoDbAsyncClient, config.getDynamoDbTables().getEcSignedPreKeys().getTableName()),
        new RepeatedUseKEMSignedPreKeyStore(dynamoDbAsyncClient, config.getDynamoDbTables().getKemLastResortKeys().getTableName()));
    MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(dynamoDbClient, dynamoDbAsyncClient,
        config.getDynamoDbTables().getMessages().getTableName(),
        config.getDynamoDbTables().getMessages().getExpiration(),
        messageDeletionAsyncExecutor);
    RemoteConfigs remoteConfigs = new RemoteConfigs(dynamoDbClient,
        config.getDynamoDbTables().getRemoteConfig().getTableName());
    PushChallengeDynamoDb pushChallengeDynamoDb = new PushChallengeDynamoDb(dynamoDbClient,
        config.getDynamoDbTables().getPushChallenge().getTableName());
    ReportMessageDynamoDb reportMessageDynamoDb = new ReportMessageDynamoDb(dynamoDbClient, dynamoDbAsyncClient,
        config.getDynamoDbTables().getReportMessage().getTableName(),
        config.getReportMessageConfiguration().getReportTtl());
    RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        config.getDynamoDbTables().getRegistrationRecovery().getTableName(),
        config.getDynamoDbTables().getRegistrationRecovery().getExpiration(),
        dynamoDbAsyncClient,
        clock);
    ClientPublicKeys clientPublicKeys =
        new ClientPublicKeys(dynamoDbAsyncClient, config.getDynamoDbTables().getClientPublicKeys().getTableName());

    final VerificationSessions verificationSessions = new VerificationSessions(dynamoDbAsyncClient,
        config.getDynamoDbTables().getVerificationSessions().getTableName(), clock);

    final ClientResources sharedClientResources = ClientResources.builder()
        .commandLatencyRecorder(
            new MicrometerCommandLatencyRecorder(Metrics.globalRegistry, MicrometerOptions.builder().build()))
        .build();
    ConnectionEventLogger.logConnectionEvents(sharedClientResources);

    FaultTolerantRedisClusterClient cacheCluster = config.getCacheClusterConfiguration()
        .build("main_cache", sharedClientResources.mutate());
    FaultTolerantRedisClusterClient messagesCluster =
        config.getMessageCacheConfiguration().getRedisClusterConfiguration()
            .build("messages", sharedClientResources.mutate());
    FaultTolerantRedisClusterClient pushSchedulerCluster = config.getPushSchedulerCluster().build("push_scheduler",
        sharedClientResources.mutate());
    FaultTolerantRedisClusterClient rateLimitersCluster = config.getRateLimitersCluster().build("rate_limiters",
        sharedClientResources.mutate());

    FaultTolerantRedisClient pubsubClient =
        config.getRedisPubSubConfiguration().build("pubsub", sharedClientResources);

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
    ExecutorService apnSenderExecutor = environment.lifecycle().executorService(name(getClass(), "apnSender-%d"))
        .maxThreads(1).minThreads(1).build();
    ExecutorService fcmSenderExecutor = environment.lifecycle().executorService(name(getClass(), "fcmSender-%d"))
        .maxThreads(32).minThreads(32).workQueue(fcmSenderQueue).build();
    ExecutorService secureValueRecovery2ServiceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "secureValueRecoveryService2-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService storageServiceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "storageService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService virtualThreadEventLoggerExecutor = environment.lifecycle()
        .executorService(name(getClass(), "virtualThreadEventLogger-%d")).minThreads(1).maxThreads(1).build();
    ExecutorService asyncOperationQueueingExecutor = environment.lifecycle()
        .executorService(name(getClass(), "asyncOperationQueueing-%d")).minThreads(1).maxThreads(1).build();
    ScheduledExecutorService secureValueRecoveryServiceRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "secureValueRecoveryServiceRetry-%d")).threads(1).build();
    ScheduledExecutorService storageServiceRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "storageServiceRetry-%d")).threads(1).build();
    ScheduledExecutorService remoteStorageRetryExecutor = environment.lifecycle()
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
    // unbounded executor (same as cachedThreadPool)
    ExecutorService remoteStorageHttpExecutor = environment.lifecycle()
        .executorService(name(getClass(), "remoteStorage-%d"))
        .minThreads(0)
        .maxThreads(Integer.MAX_VALUE)
        .workQueue(new SynchronousQueue<>())
        .keepAliveTime(io.dropwizard.util.Duration.seconds(60L))
        .build();
    ExecutorService cloudflareTurnHttpExecutor = environment.lifecycle()
        .executorService(name(getClass(), "cloudflareTurn-%d"))
        .maxThreads(2)
        .minThreads(2)
        .build();
    ExecutorService googlePlayBillingExecutor = environment.lifecycle()
        .virtualExecutorService(name(getClass(), "googlePlayBilling-%d"));
    ExecutorService appleAppStoreExecutor = environment.lifecycle()
        .virtualExecutorService(name(getClass(), "appleAppStore-%d"));
    ExecutorService clientEventExecutor = environment.lifecycle()
        .virtualExecutorService(name(getClass(), "clientEvent-%d"));
    ExecutorService disconnectionRequestListenerExecutor = environment.lifecycle()
        .virtualExecutorService(name(getClass(), "disconnectionRequest-%d"));

    ScheduledExecutorService appleAppStoreRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "appleAppStoreRetry-%d")).threads(1).build();
    ScheduledExecutorService subscriptionProcessorRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "subscriptionProcessorRetry-%d")).threads(1).build();
    ScheduledExecutorService cloudflareTurnRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "cloudflareTurnRetry-%d")).threads(1).build();
    ScheduledExecutorService messagePollExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "messagePollExecutor-%d")).threads(1).build();
    ScheduledExecutorService provisioningWebsocketTimeoutExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "provisioningWebsocketTimeout-%d")).threads(1).build();

    final ManagedNioEventLoopGroup dnsResolutionEventLoopGroup = new ManagedNioEventLoopGroup();
    final DnsNameResolver cloudflareDnsResolver = new DnsNameResolverBuilder(dnsResolutionEventLoopGroup.next())
            .resolvedAddressTypes(ResolvedAddressTypes.IPV6_PREFERRED)
            .completeOncePreferredResolved(false)
            .channelType(NioDatagramChannel.class)
            .socketChannelType(NioSocketChannel.class)
            .build();

    ExternalServiceCredentialsGenerator directoryV2CredentialsGenerator = DirectoryV2Controller.credentialsGenerator(
        config.getDirectoryV2Configuration().getDirectoryV2ClientConfiguration());
    ExternalServiceCredentialsGenerator storageCredentialsGenerator = SecureStorageController.credentialsGenerator(
        config.getSecureStorageServiceConfiguration());
    ExternalServiceCredentialsGenerator paymentsCredentialsGenerator = PaymentsController.credentialsGenerator(
        config.getPaymentsServiceConfiguration());
    ExternalServiceCredentialsGenerator svr2CredentialsGenerator = SecureValueRecovery2Controller.credentialsGenerator(
            config.getSvr2Configuration());

    ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(
        dynamicConfigurationManager);
    RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords);
    UsernameHashZkProofVerifier usernameHashZkProofVerifier = new UsernameHashZkProofVerifier();

    RegistrationServiceClient registrationServiceClient = config.getRegistrationServiceConfiguration()
        .build(environment, registrationCallbackExecutor, registrationIdentityTokenRefreshExecutor);
    // FLT(uoemai): Key transparency is disabled in the prototype.
    // KeyTransparencyServiceClient keyTransparencyServiceClient = new KeyTransparencyServiceClient(
    //     config.getKeyTransparencyServiceConfiguration().host(),
    //     config.getKeyTransparencyServiceConfiguration().port(),
    //     config.getKeyTransparencyServiceConfiguration().tlsCertificate(),
    //     config.getKeyTransparencyServiceConfiguration().clientCertificate(),
    //     config.getKeyTransparencyServiceConfiguration().clientPrivateKey().value());
    // FLT(uoemai): Secure value recovery is disabled in the prototype.
    // SecureValueRecovery2Client secureValueRecovery2Client = new SecureValueRecovery2Client(svr2CredentialsGenerator,
    //     secureValueRecovery2ServiceExecutor, secureValueRecoveryServiceRetryExecutor, config.getSvr2Configuration());
    ValueRecovery2Client insecureValueRecovery2Client = new InsecureValueRecovery2Client();
    // FLT(uoemai): Secure storage is disabled in the prototype.
    // SecureStorageClient secureStorageClient = new SecureStorageClient(storageCredentialsGenerator,
    //     storageServiceExecutor, storageServiceRetryExecutor, config.getSecureStorageServiceConfiguration());
    StorageClient insecureStorageClient = new InsecureStorageClient();
    DisconnectionRequestManager disconnectionRequestManager = new DisconnectionRequestManager(pubsubClient, disconnectionRequestListenerExecutor);
    ProfilesManager profilesManager = new ProfilesManager(profiles, cacheCluster, asyncCdnS3Client, config.getCdnConfiguration().bucket());
    MessagesCache messagesCache = new MessagesCache(messagesCluster, messageDeliveryScheduler,
        messageDeletionAsyncExecutor, clock);
    ClientReleaseManager clientReleaseManager = new ClientReleaseManager(clientReleases,
        recurringJobExecutor,
        config.getClientReleaseConfiguration().refreshInterval(),
        Clock.systemUTC());
    ReportMessageManager reportMessageManager = new ReportMessageManager(reportMessageDynamoDb, rateLimitersCluster,
        config.getReportMessageConfiguration().getCounterTtl());
    MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, reportMessageManager,
        messageDeletionAsyncExecutor, Clock.systemUTC());
    AccountLockManager accountLockManager = new AccountLockManager(dynamoDbClient,
        config.getDynamoDbTables().getDeletedAccountsLock().getTableName());
    ClientPublicKeysManager clientPublicKeysManager =
        new ClientPublicKeysManager(clientPublicKeys, accountLockManager, accountLockExecutor);
    AccountsManager accountsManager = new AccountsManager(accounts, phoneNumberIdentifiers, cacheCluster,
        pubsubClient, accountLockManager, keysManager, messagesManager, profilesManager,
        insecureStorageClient, insecureValueRecovery2Client, disconnectionRequestManager,
        registrationRecoveryPasswordsManager, clientPublicKeysManager, accountLockExecutor, messagePollExecutor,
        clock, config.getLinkDeviceSecretConfiguration().secret().value(), dynamicConfigurationManager);
    RemoteConfigsManager remoteConfigsManager = new RemoteConfigsManager(remoteConfigs);
    // FLT(uoemai): Notification providers replaced by dummy logger during development.
    // APNSender apnSender = new APNSender(apnSenderExecutor, config.getApnConfiguration());
    // FcmSender fcmSender = new FcmSender(fcmSenderExecutor, config.getFcmConfiguration().credentials().value());
    DummySender apnSender = new DummySender("APN");
    DummySender fcmSender = new DummySender("FCM");
    PushNotificationScheduler pushNotificationScheduler = new PushNotificationScheduler(pushSchedulerCluster,
        apnSender, fcmSender, accountsManager, 0, 0);
    PushNotificationManager pushNotificationManager =
        new PushNotificationManager(accountsManager, apnSender, fcmSender, pushNotificationScheduler);
    WebSocketConnectionEventManager webSocketConnectionEventManager =
        new WebSocketConnectionEventManager(accountsManager, pushNotificationManager, messagesCluster, clientEventExecutor, asyncOperationQueueingExecutor);
    RateLimiters rateLimiters = RateLimiters.create(dynamicConfigurationManager, rateLimitersCluster);
    // FLT(uoemai): Device provisioning is disabled in the prototype.
    // ProvisioningManager provisioningManager = new ProvisioningManager(pubsubClient);
    IssuedReceiptsManager issuedReceiptsManager = new IssuedReceiptsManager(
        config.getDynamoDbTables().getIssuedReceipts().getTableName(),
        config.getDynamoDbTables().getIssuedReceipts().getExpiration(),
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getIssuedReceipts().getGenerator(),
        config.getDynamoDbTables().getIssuedReceipts().getmaxIssuedReceiptsPerPaymentId());
    OneTimeDonationsManager oneTimeDonationsManager = new OneTimeDonationsManager(
        config.getDynamoDbTables().getOnetimeDonations().getTableName(), config.getDynamoDbTables().getOnetimeDonations().getExpiration(), dynamoDbAsyncClient);
    RedeemedReceiptsManager redeemedReceiptsManager = new RedeemedReceiptsManager(clock,
        config.getDynamoDbTables().getRedeemedReceipts().getTableName(),
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getRedeemedReceipts().getExpiration());
    Subscriptions subscriptions = new Subscriptions(
        config.getDynamoDbTables().getSubscriptions().getTableName(), dynamoDbAsyncClient);
    MessageDeliveryLoopMonitor messageDeliveryLoopMonitor =
        config.logMessageDeliveryLoops() ? new RedisMessageDeliveryLoopMonitor(rateLimitersCluster) : new NoopMessageDeliveryLoopMonitor();

    disconnectionRequestManager.addListener(webSocketConnectionEventManager);

    final RegistrationLockVerificationManager registrationLockVerificationManager = new RegistrationLockVerificationManager(
        accountsManager, disconnectionRequestManager, svr2CredentialsGenerator, registrationRecoveryPasswordsManager,
        pushNotificationManager, rateLimiters);

    final ReportedMessageMetricsListener reportedMessageMetricsListener = new ReportedMessageMetricsListener(
        accountsManager);
    reportMessageManager.addListener(reportedMessageMetricsListener);

    final AccountAuthenticator accountAuthenticator = new AccountAuthenticator(accountsManager);

    final MessageSender messageSender = new MessageSender(messagesManager, pushNotificationManager, experimentEnrollmentManager);
    final ReceiptSender receiptSender = new ReceiptSender(accountsManager, messageSender, receiptSenderExecutor);
    final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager = new CloudflareTurnCredentialsManager(
        config.getTurnConfiguration().cloudflare().apiToken().value(),
        config.getTurnConfiguration().cloudflare().endpoint(),
        config.getTurnConfiguration().cloudflare().requestedCredentialTtl(),
        config.getTurnConfiguration().cloudflare().clientCredentialTtl(),
        config.getTurnConfiguration().cloudflare().urls(),
        config.getTurnConfiguration().cloudflare().urlsWithIps(),
        config.getTurnConfiguration().cloudflare().hostname(),
        config.getTurnConfiguration().cloudflare().numHttpClients(),
        config.getTurnConfiguration().cloudflare().circuitBreaker(),
        cloudflareTurnHttpExecutor,
        config.getTurnConfiguration().cloudflare().retry(),
        cloudflareTurnRetryExecutor,
        cloudflareDnsResolver
        );

    final CardinalityEstimator messageByteLimitCardinalityEstimator = new CardinalityEstimator(
        rateLimitersCluster,
        "message_byte_limit",
        config.getMessageByteLimitCardinalityEstimator().period());

    PushChallengeManager pushChallengeManager = new PushChallengeManager(pushNotificationManager,
        pushChallengeDynamoDb);

    ChangeNumberManager changeNumberManager = new ChangeNumberManager(messageSender, accountsManager, Clock.systemUTC());

    HttpClient currencyClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(10)).build();
    FixerClient fixerClient = config.getPaymentsServiceConfiguration().externalClients()
        .buildFixerClient(currencyClient);
    CoinGeckoClient coinGeckoClient = config.getPaymentsServiceConfiguration().externalClients()
        .buildCoinGeckoClient(currencyClient);
    CurrencyConversionManager currencyManager = new CurrencyConversionManager(fixerClient, coinGeckoClient,
        cacheCluster, config.getPaymentsServiceConfiguration().paymentCurrencies(), recurringJobExecutor, Clock.systemUTC());
    VirtualThreadPinEventMonitor virtualThreadPinEventMonitor = new VirtualThreadPinEventMonitor(
        virtualThreadEventLoggerExecutor,
        () -> dynamicConfigurationManager.getConfiguration().getVirtualThreads().allowedPinEvents(),
        config.getVirtualThreadConfiguration().pinEventThreshold());

    // FLT(uoemai): All forms of payment are disabled in the prototype.
    //  StripeManager stripeManager = new StripeManager(config.getStripe().apiKey().value(), subscriptionProcessorExecutor,
    //      config.getStripe().idempotencyKeyGenerator().value(), config.getStripe().boostDescription(), config.getStripe().supportedCurrenciesByPaymentMethod());
    //  BraintreeManager braintreeManager = new BraintreeManager(config.getBraintree().merchantId(),
    //      config.getBraintree().publicKey(), config.getBraintree().privateKey().value(),
    //      config.getBraintree().environment(),
    //      config.getBraintree().supportedCurrenciesByPaymentMethod(), config.getBraintree().merchantAccounts(),
    //      config.getBraintree().graphqlUrl(), currencyManager, config.getBraintree().pubSubPublisher().build(),
    //      config.getBraintree().circuitBreaker(), subscriptionProcessorExecutor,
    //      subscriptionProcessorRetryExecutor);
    //  GooglePlayBillingManager googlePlayBillingManager = new GooglePlayBillingManager(
    //      new ByteArrayInputStream(config.getGooglePlayBilling().credentialsJson().value().getBytes(StandardCharsets.UTF_8)),
    //      config.getGooglePlayBilling().packageName(),
    //      config.getGooglePlayBilling().applicationName(),
    //      config.getGooglePlayBilling().productIdToLevel(),
    //      googlePlayBillingExecutor);
    //  AppleAppStoreManager appleAppStoreManager = new AppleAppStoreManager(
    //      config.getAppleAppStore().env(), config.getAppleAppStore().bundleId(), config.getAppleAppStore().appAppleId(),
    //      config.getAppleAppStore().issuerId(), config.getAppleAppStore().keyId(),
    //      config.getAppleAppStore().encodedKey().value(), config.getAppleAppStore().subscriptionGroupId(),
    //      config.getAppleAppStore().productIdToLevel(),
    //      config.getAppleAppStore().appleRootCerts(),
    //      config.getAppleAppStore().retry(), appleAppStoreExecutor, appleAppStoreRetryExecutor);

    environment.lifecycle().manage(apnSender);
    environment.lifecycle().manage(pushNotificationScheduler);
    // FLT(uoemai): Device provisioning is disabled in the prototype.
    // environment.lifecycle().manage(provisioningManager);
    environment.lifecycle().manage(disconnectionRequestManager);
    environment.lifecycle().manage(webSocketConnectionEventManager);
    environment.lifecycle().manage(currencyManager);
    environment.lifecycle().manage(registrationServiceClient);
    // FLT(uoemai): Key transparency is disabled in the prototype.
    // environment.lifecycle().manage(keyTransparencyServiceClient);
    environment.lifecycle().manage(clientReleaseManager);
    environment.lifecycle().manage(virtualThreadPinEventMonitor);
    environment.lifecycle().manage(accountsManager);

    // FLT(uoemai): Cloud uploads are disabled in the prototype.
    // final GcsAttachmentGenerator gcsAttachmentGenerator = new GcsAttachmentGenerator(
    //     config.getGcpAttachmentsConfiguration().domain(),
    //     config.getGcpAttachmentsConfiguration().email(),
    //     config.getGcpAttachmentsConfiguration().maxSizeInBytes(),
    //     config.getGcpAttachmentsConfiguration().pathPrefix(),
    //     config.getGcpAttachmentsConfiguration().rsaSigningKey().value());
    final DummyAttachmentGenerator gcsAttachmentGenerator = new DummyAttachmentGenerator();

    PostPolicyGenerator profileCdnPolicyGenerator = new PostPolicyGenerator(config.getCdnConfiguration().region(),
        config.getCdnConfiguration().bucket(), config.getCdnConfiguration().credentials().accessKeyId().value());
    PolicySigner profileCdnPolicySigner = new PolicySigner(
        config.getCdnConfiguration().credentials().secretAccessKey().value(),
        config.getCdnConfiguration().region());

    ServerSecretParams zkSecretParams = new ServerSecretParams(config.getZkConfig().serverSecret().value());
    GenericServerSecretParams callingGenericZkSecretParams = new GenericServerSecretParams(config.getCallingZkConfig().serverSecret().value());
    GenericServerSecretParams backupsGenericZkSecretParams = new GenericServerSecretParams(config.getBackupsZkConfig().serverSecret().value());
    ServerZkProfileOperations zkProfileOperations = new ServerZkProfileOperations(zkSecretParams);
    ServerZkAuthOperations zkAuthOperations = new ServerZkAuthOperations(zkSecretParams);
    ServerZkReceiptOperations zkReceiptOperations = new ServerZkReceiptOperations(zkSecretParams);

    TusAttachmentGenerator tusAttachmentGenerator = new TusAttachmentGenerator(config.getTus());
    Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator = new Cdn3BackupCredentialGenerator(config.getTus());
    BackupAuthManager backupAuthManager = new BackupAuthManager(experimentEnrollmentManager, rateLimiters,
        accountsManager, zkReceiptOperations, redeemedReceiptsManager, backupsGenericZkSecretParams, clock);
    BackupsDb backupsDb = new BackupsDb(
        dynamoDbAsyncClient,
        config.getDynamoDbTables().getBackups().getTableName(),
        clock);
    final Cdn3RemoteStorageManager cdn3RemoteStorageManager = new Cdn3RemoteStorageManager(
        remoteStorageHttpExecutor,
        remoteStorageRetryExecutor,
        config.getCdn3StorageManagerConfiguration());
    BackupManager backupManager = new BackupManager(
        backupsDb,
        backupsGenericZkSecretParams,
        rateLimiters,
        tusAttachmentGenerator,
        cdn3BackupCredentialGenerator,
        cdn3RemoteStorageManager,
        clock);

    final AppleDeviceChecks appleDeviceChecks = new AppleDeviceChecks(
        dynamoDbClient,
        DeviceCheckManager.createObjectConverter(),
        config.getDynamoDbTables().getAppleDeviceChecks().getTableName(),
        config.getDynamoDbTables().getAppleDeviceCheckPublicKeys().getTableName());
    final DeviceCheckManager deviceCheckManager = new DeviceCheckManager(new AppleDeviceCheckTrustAnchor());
    deviceCheckManager.getAttestationDataValidator().setProduction(config.getAppleDeviceCheck().production());
    final AppleDeviceCheckManager appleDeviceCheckManager = new AppleDeviceCheckManager(
        appleDeviceChecks,
        cacheCluster,
        deviceCheckManager,
        config.getAppleDeviceCheck().teamId(),
        config.getAppleDeviceCheck().bundleId());

    final GrpcClientConnectionManager grpcClientConnectionManager = new GrpcClientConnectionManager();

    disconnectionRequestManager.addListener(grpcClientConnectionManager);

    final ManagedDefaultEventLoopGroup localEventLoopGroup = new ManagedDefaultEventLoopGroup();

    final RemoteDeprecationFilter remoteDeprecationFilter = new RemoteDeprecationFilter(dynamicConfigurationManager);
    final MetricCollectingServerInterceptor metricCollectingServerInterceptor =
        new MetricCollectingServerInterceptor(Metrics.globalRegistry);

    final ErrorMappingInterceptor errorMappingInterceptor = new ErrorMappingInterceptor();
    final RequestAttributesInterceptor requestAttributesInterceptor =
        new RequestAttributesInterceptor(grpcClientConnectionManager);

    final ValidatingInterceptor validatingInterceptor = new ValidatingInterceptor();

    final LocalAddress anonymousGrpcServerAddress = new LocalAddress("grpc-anonymous");
    final LocalAddress authenticatedGrpcServerAddress = new LocalAddress("grpc-authenticated");

    final ManagedLocalGrpcServer anonymousGrpcServer = new ManagedLocalGrpcServer(anonymousGrpcServerAddress, localEventLoopGroup) {
      @Override
      protected void configureServer(final ServerBuilder<?> serverBuilder) {
        // Note: interceptors run in the reverse order they are added; the remote deprecation filter
        // depends on the user-agent context so it has to come first here!
        // http://grpc.github.io/grpc-java/javadoc/io/grpc/ServerBuilder.html#intercept-io.grpc.ServerInterceptor-
        serverBuilder
            .intercept(
                new ExternalRequestFilter(config.getExternalRequestFilterConfiguration().permittedInternalRanges(),
                    config.getExternalRequestFilterConfiguration().grpcMethods()))
            .intercept(validatingInterceptor)
            // TODO: specialize metrics with user-agent platform
            .intercept(metricCollectingServerInterceptor)
            .intercept(errorMappingInterceptor)
            .intercept(remoteDeprecationFilter)
            .intercept(requestAttributesInterceptor)
            .intercept(new ProhibitAuthenticationInterceptor(grpcClientConnectionManager))
            .addService(new AccountsAnonymousGrpcService(accountsManager, rateLimiters))
            .addService(new KeysAnonymousGrpcService(accountsManager, keysManager, zkSecretParams, Clock.systemUTC()))
            .addService(new PaymentsGrpcService(currencyManager))
            .addService(ExternalServiceCredentialsAnonymousGrpcService.create(accountsManager, config))
            .addService(new ProfileAnonymousGrpcService(accountsManager, profilesManager, profileBadgeConverter, zkSecretParams));
      }
    };

    final ManagedLocalGrpcServer authenticatedGrpcServer = new ManagedLocalGrpcServer(authenticatedGrpcServerAddress, localEventLoopGroup) {
      @Override
      protected void configureServer(final ServerBuilder<?> serverBuilder) {
        // Note: interceptors run in the reverse order they are added; the remote deprecation filter
        // depends on the user-agent context so it has to come first here!
        // http://grpc.github.io/grpc-java/javadoc/io/grpc/ServerBuilder.html#intercept-io.grpc.ServerInterceptor-
        serverBuilder
            // TODO: specialize metrics with user-agent platform
            .intercept(validatingInterceptor)
            .intercept(metricCollectingServerInterceptor)
            .intercept(errorMappingInterceptor)
            .intercept(remoteDeprecationFilter)
            .intercept(requestAttributesInterceptor)
            .intercept(new RequireAuthenticationInterceptor(grpcClientConnectionManager))
            .addService(new AccountsGrpcService(accountsManager, rateLimiters, usernameHashZkProofVerifier, registrationRecoveryPasswordsManager))
            .addService(ExternalServiceCredentialsGrpcService.createForAllExternalServices(config, rateLimiters))
            .addService(new KeysGrpcService(accountsManager, keysManager, rateLimiters))
            .addService(new ProfileGrpcService(clock, accountsManager, profilesManager, dynamicConfigurationManager,
                config.getBadges(), profileCdnPolicyGenerator, profileCdnPolicySigner, profileBadgeConverter, rateLimiters, zkProfileOperations));
      }
    };

    @Nullable final X509Certificate[] noiseWebSocketTlsCertificateChain;
    @Nullable final PrivateKey noiseWebSocketTlsPrivateKey;

    if (config.getNoiseTunnelConfiguration().tlsKeyStoreFile() != null &&
        config.getNoiseTunnelConfiguration().tlsKeyStoreEntryAlias() != null &&
        config.getNoiseTunnelConfiguration().tlsKeyStorePassword() != null) {

      try (final FileInputStream websocketNoiseTunnelTlsKeyStoreInputStream = new FileInputStream(config.getNoiseTunnelConfiguration().tlsKeyStoreFile())) {
        final KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(websocketNoiseTunnelTlsKeyStoreInputStream, config.getNoiseTunnelConfiguration().tlsKeyStorePassword().value().toCharArray());

        final KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) keyStore.getEntry(
            config.getNoiseTunnelConfiguration().tlsKeyStoreEntryAlias(),
            new KeyStore.PasswordProtection(config.getNoiseTunnelConfiguration().tlsKeyStorePassword().value().toCharArray()));

        noiseWebSocketTlsCertificateChain =
            Arrays.copyOf(privateKeyEntry.getCertificateChain(), privateKeyEntry.getCertificateChain().length, X509Certificate[].class);

        noiseWebSocketTlsPrivateKey = privateKeyEntry.getPrivateKey();
      }
    } else {
      noiseWebSocketTlsCertificateChain = null;
      noiseWebSocketTlsPrivateKey = null;
    }

    final ExecutorService noiseWebSocketDelegatedTaskExecutor = environment.lifecycle()
        .executorService(name(getClass(), "noiseWebsocketDelegatedTask-%d"))
        .minThreads(8)
        .maxThreads(8)
        .allowCoreThreadTimeOut(false)
        .build();

    final ManagedNioEventLoopGroup noiseTunnelEventLoopGroup = new ManagedNioEventLoopGroup();

    final NoiseWebSocketTunnelServer noiseWebSocketTunnelServer = new NoiseWebSocketTunnelServer(
        config.getNoiseTunnelConfiguration().webSocketPort(),
        noiseWebSocketTlsCertificateChain,
        noiseWebSocketTlsPrivateKey,
        noiseTunnelEventLoopGroup,
        noiseWebSocketDelegatedTaskExecutor,
        grpcClientConnectionManager,
        clientPublicKeysManager,
        config.getNoiseTunnelConfiguration().noiseStaticKeyPair(),
        authenticatedGrpcServerAddress,
        anonymousGrpcServerAddress,
        config.getNoiseTunnelConfiguration().recognizedProxySecret().value());

    final NoiseDirectTunnelServer noiseDirectTunnelServer = new NoiseDirectTunnelServer(
        config.getNoiseTunnelConfiguration().directPort(),
        noiseTunnelEventLoopGroup,
        grpcClientConnectionManager,
        clientPublicKeysManager,
        config.getNoiseTunnelConfiguration().noiseStaticKeyPair(),
        authenticatedGrpcServerAddress,
        anonymousGrpcServerAddress);

    environment.lifecycle().manage(localEventLoopGroup);
    environment.lifecycle().manage(dnsResolutionEventLoopGroup);
    environment.lifecycle().manage(anonymousGrpcServer);
    environment.lifecycle().manage(authenticatedGrpcServer);
    environment.lifecycle().manage(noiseTunnelEventLoopGroup);
    environment.lifecycle().manage(noiseWebSocketTunnelServer);
    environment.lifecycle().manage(noiseDirectTunnelServer);

    final List<Filter> filters = new ArrayList<>();
    filters.add(remoteDeprecationFilter);
    filters.add(new RemoteAddressFilter());
    filters.add(new TimestampResponseFilter());

    for (Filter filter : filters) {
      environment.servlets()
          .addFilter(filter.getClass().getSimpleName(), filter)
          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");
    }

    if (!config.getExternalRequestFilterConfiguration().paths().isEmpty()) {
      environment.servlets().addFilter(ExternalRequestFilter.class.getSimpleName(),
              new ExternalRequestFilter(config.getExternalRequestFilterConfiguration().permittedInternalRanges(),
                  config.getExternalRequestFilterConfiguration().grpcMethods()))
          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true,
              config.getExternalRequestFilterConfiguration().paths().toArray(new String[]{}));
    }

    final AuthFilter<BasicCredentials, AuthenticatedDevice> accountAuthFilter =
        new BasicCredentialAuthFilter.Builder<AuthenticatedDevice>()
            .setAuthenticator(accountAuthenticator)
            .buildAuthFilter();

    final String websocketServletPath = "/v1/websocket/";
    final String provisioningWebsocketServletPath = "/v1/websocket/provisioning/";

    final MetricsHttpChannelListener metricsHttpChannelListener = new MetricsHttpChannelListener(clientReleaseManager,
        Set.of(websocketServletPath, provisioningWebsocketServletPath, "/health-check"));
    metricsHttpChannelListener.configure(environment);
    final MessageMetrics messageMetrics = new MessageMetrics();
    final BackupMetrics backupMetrics = new BackupMetrics();

    // BufferingInterceptor is needed on the base environment but not the WebSocketEnvironment,
    // because we handle serialization of http responses on the websocket on our own and can
    // compute content lengths without it
    environment.jersey().register(new BufferingInterceptor());
    environment.jersey().register(new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager));

    environment.jersey().register(new VirtualExecutorServiceProvider("managed-async-virtual-thread-"));
    environment.jersey().register(new RateLimitByIpFilter(rateLimiters));
    environment.jersey().register(new RequestStatisticsFilter(TrafficSource.HTTP));
    environment.jersey().register(MultiRecipientMessageProvider.class);
    environment.jersey().register(new AuthDynamicFeature(accountAuthFilter));
    environment.jersey().register(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class));
    environment.jersey().register(new TimestampResponseFilter());

    ///
    WebSocketEnvironment<AuthenticatedDevice> webSocketEnvironment = new WebSocketEnvironment<>(environment,
        config.getWebSocketConfiguration(), Duration.ofMillis(90000));
    webSocketEnvironment.jersey().register(new VirtualExecutorServiceProvider("managed-async-websocket-virtual-thread-"));
    webSocketEnvironment.setAuthenticator(new WebSocketAccountAuthenticator(accountAuthenticator));
    webSocketEnvironment.setAuthenticatedWebSocketUpgradeFilter(new IdlePrimaryDeviceAuthenticatedWebSocketUpgradeFilter(
        config.idlePrimaryDeviceReminderConfiguration().minIdleDuration(), Clock.systemUTC()));
    webSocketEnvironment.setConnectListener(
        new AuthenticatedConnectListener(accountsManager, receiptSender, messagesManager, messageMetrics, pushNotificationManager,
            pushNotificationScheduler, webSocketConnectionEventManager, websocketScheduledExecutor,
            messageDeliveryScheduler, clientReleaseManager, messageDeliveryLoopMonitor, experimentEnrollmentManager));
    webSocketEnvironment.jersey().register(new RateLimitByIpFilter(rateLimiters));
    webSocketEnvironment.jersey().register(new RequestStatisticsFilter(TrafficSource.WEBSOCKET));
    webSocketEnvironment.jersey().register(MultiRecipientMessageProvider.class);
    webSocketEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET, clientReleaseManager));
    webSocketEnvironment.jersey().register(new KeepAliveController(webSocketConnectionEventManager));
    webSocketEnvironment.jersey().register(new TimestampResponseFilter());

    final List<SpamFilter> spamFilters = ServiceLoader.load(SpamFilter.class)
        .stream()
        .map(ServiceLoader.Provider::get)
        .flatMap(filter -> {
          try {
            filter.configure(config.getSpamFilterConfiguration().getEnvironment(), environment.getValidator());
            return Stream.of(filter);
          } catch (Exception e) {
            log.warn("Failed to register spam filter: {}", filter.getClass().getName(), e);
            return Stream.empty();
          }
        })
        .toList();
    if (spamFilters.size() > 1) {
      log.warn("Multiple spam report token providers found. Using the first.");
    }
    final Optional<SpamFilter> spamFilter = spamFilters.stream().findFirst();
    if (spamFilter.isEmpty()) {
      log.warn("No spam filters installed");
    }
    final SpamChecker spamChecker = spamFilter
        .map(SpamFilter::getSpamChecker)
        .orElseGet(() -> {
          log.warn("No spam-checkers found; using default (no-op) provider as a default");
          return SpamChecker.noop();
        });
    final ChallengeConstraintChecker challengeConstraintChecker = spamFilter
        .map(SpamFilter::getChallengeConstraintChecker)
        .orElseGet(() -> {
          log.warn("No challenge-constraint-checkers found; using default (no-op) provider as a default");
          return ChallengeConstraintChecker.noop();
        });
    final RegistrationFraudChecker registrationFraudChecker = spamFilter
        .map(SpamFilter::getRegistrationFraudChecker)
        .orElseGet(() -> {
          log.warn("No registration-fraud-checkers found; using default (no-op) provider as a default");
          return RegistrationFraudChecker.noop();
        });
    final RegistrationRecoveryChecker registrationRecoveryChecker = spamFilter
        .map(SpamFilter::getRegistrationRecoveryChecker)
        .orElseGet(() -> {
          log.warn("No registration-recovery-checkers found; using default (no-op) provider as a default");
          return RegistrationRecoveryChecker.noop();
        });
    final Function<String, CaptchaClient> captchaClientSupplier = spamFilter
        .map(SpamFilter::getCaptchaClientSupplier)
        .orElseGet(() -> {
          log.warn("No captcha clients found; using default (no-op) client as default");
          return ignored -> CaptchaClient.noop();
        });

    spamFilter.map(SpamFilter::getReportedMessageListener).ifPresent(reportMessageManager::addListener);

    final HttpClient shortCodeRetrieverHttpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(10)).build();
    final ShortCodeExpander shortCodeRetriever = new ShortCodeExpander(shortCodeRetrieverHttpClient, config.getShortCodeRetrieverConfiguration().baseUrl());
    final CaptchaChecker captchaChecker = new CaptchaChecker(shortCodeRetriever, captchaClientSupplier);

    final RegistrationCaptchaManager registrationCaptchaManager = new RegistrationCaptchaManager(captchaChecker);

    final RateLimitChallengeManager rateLimitChallengeManager = new RateLimitChallengeManager(pushChallengeManager,
        captchaChecker, rateLimiters, spamFilter.map(SpamFilter::getRateLimitChallengeListener).stream().toList());

    spamFilter.ifPresent(filter -> {
      environment.lifecycle().manage(filter);
      log.info("Registered spam filter: {}", filter.getClass().getName());
    });

    final PersistentTimer persistentTimer = new PersistentTimer(rateLimitersCluster, clock);

    final PhoneVerificationTokenManager phoneVerificationTokenManager = new PhoneVerificationTokenManager(
        phoneNumberIdentifiers, registrationServiceClient, registrationRecoveryPasswordsManager, registrationRecoveryChecker);
    final List<Object> commonControllers = Lists.newArrayList(
        new AccountController(accountsManager, rateLimiters, registrationRecoveryPasswordsManager,
            usernameHashZkProofVerifier),
        new AccountControllerV2(accountsManager, changeNumberManager, phoneVerificationTokenManager,
            registrationLockVerificationManager, rateLimiters),
        new AttachmentControllerV4(rateLimiters, gcsAttachmentGenerator, tusAttachmentGenerator,
            experimentEnrollmentManager),
        new ArchiveController(accountsManager, backupAuthManager, backupManager, backupMetrics),
        new CallRoutingControllerV2(rateLimiters, cloudflareTurnCredentialsManager),
        new CallLinkController(rateLimiters, callingGenericZkSecretParams),
        new CertificateController(accountsManager, new CertificateGenerator(config.getDeliveryCertificate().certificate(),
            config.getDeliveryCertificate().ecPrivateKey(), config.getDeliveryCertificate().expiresDays()),
            zkAuthOperations, callingGenericZkSecretParams, clock),
        new ChallengeController(accountsManager, rateLimitChallengeManager, challengeConstraintChecker),
        new DeviceController(accountsManager, clientPublicKeysManager, rateLimiters, persistentTimer, config.getMaxDevices()),
        new DeviceCheckController(clock, accountsManager, backupAuthManager, appleDeviceCheckManager, rateLimiters,
            config.getDeviceCheck().backupRedemptionLevel(),
            config.getDeviceCheck().backupRedemptionDuration()),
        new DirectoryV2Controller(directoryV2CredentialsGenerator),
        new DonationController(clock, zkReceiptOperations, redeemedReceiptsManager, accountsManager, config.getBadges(),
            ReceiptCredentialPresentation::new),
        new KeysController(rateLimiters, keysManager, accountsManager, zkSecretParams, Clock.systemUTC()),
        // FLT(uoemai): Disable key transparency for the prototype.
        // new KeyTransparencyController(keyTransparencyServiceClient),
        new MessageController(rateLimiters, messageByteLimitCardinalityEstimator, messageSender, receiptSender,
            accountsManager, messagesManager, phoneNumberIdentifiers, pushNotificationManager, pushNotificationScheduler,
            reportMessageManager, messageDeliveryScheduler, clientReleaseManager,
            zkSecretParams, spamChecker, messageMetrics, messageDeliveryLoopMonitor,
            Clock.systemUTC()),
        new PaymentsController(currencyManager, paymentsCredentialsGenerator),
        new ProfileController(clock, rateLimiters, accountsManager, profilesManager, dynamicConfigurationManager,
            profileBadgeConverter, config.getBadges(), profileCdnPolicyGenerator, profileCdnPolicySigner,
            zkSecretParams, zkProfileOperations, batchIdentityCheckExecutor),
        // FLT(uoemai): Device provisioning is disabled in the prototype.
        // new ProvisioningController(rateLimiters, provisioningManager),
        new RegistrationController(accountsManager, phoneVerificationTokenManager, registrationLockVerificationManager,
            rateLimiters),
        new RemoteConfigController(remoteConfigsManager, config.getRemoteConfigConfiguration().globalConfig(), clock),
        new SecureStorageController(storageCredentialsGenerator),
        new SecureValueRecovery2Controller(svr2CredentialsGenerator, accountsManager),
        new StickerController(rateLimiters, config.getCdnConfiguration().credentials().accessKeyId().value(),
            config.getCdnConfiguration().credentials().secretAccessKey().value(), config.getCdnConfiguration().region(),
            config.getCdnConfiguration().bucket()),
        new VerificationController(registrationServiceClient, new VerificationSessionManager(verificationSessions),
            pushNotificationManager, registrationCaptchaManager, registrationRecoveryPasswordsManager,
            phoneNumberIdentifiers, rateLimiters, accountsManager, registrationFraudChecker,
            dynamicConfigurationManager, clock)
    );
    // FLT(uoemai): All forms of payment are disabled in the prototype.
    // if (config.getSubscription() != null && config.getOneTimeDonations() != null) {
    //   SubscriptionManager subscriptionManager = new SubscriptionManager(subscriptions,
    //       List.of(stripeManager, braintreeManager, googlePlayBillingManager, appleAppStoreManager),
    //       zkReceiptOperations, issuedReceiptsManager);
    //   commonControllers.add(new SubscriptionController(clock, config.getSubscription(), config.getOneTimeDonations(),
    //       subscriptionManager, stripeManager, braintreeManager, googlePlayBillingManager, appleAppStoreManager,
    //       profileBadgeConverter, bankMandateTranslator));
    //   commonControllers.add(new OneTimeDonationController(clock, config.getOneTimeDonations(), stripeManager, braintreeManager,
    //       zkReceiptOperations, issuedReceiptsManager, oneTimeDonationsManager));
    // }

    for (Object controller : commonControllers) {
      environment.jersey().register(controller);
      webSocketEnvironment.jersey().register(controller);
    }

    WebSocketEnvironment<AuthenticatedDevice> provisioningEnvironment = new WebSocketEnvironment<>(environment,
        webSocketEnvironment.getRequestLog(), Duration.ofMillis(60000));
    // FLT(uoemai): Device provisioning is disabled in the prototype.
    // provisioningEnvironment.setConnectListener(new ProvisioningConnectListener(provisioningManager, provisioningWebsocketTimeoutExecutor, Duration.ofSeconds(90)));
    provisioningEnvironment.jersey().register(new MetricsApplicationEventListener(TrafficSource.WEBSOCKET, clientReleaseManager));
    provisioningEnvironment.jersey().register(new KeepAliveController(webSocketConnectionEventManager));
    provisioningEnvironment.jersey().register(new TimestampResponseFilter());

    registerExceptionMappers(environment, webSocketEnvironment, provisioningEnvironment);

    environment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
    webSocketEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
    provisioningEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);

    JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), null);

    WebSocketResourceProviderFactory<AuthenticatedDevice> webSocketServlet = new WebSocketResourceProviderFactory<>(
        webSocketEnvironment, AuthenticatedDevice.class, config.getWebSocketConfiguration(),
        RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);
    WebSocketResourceProviderFactory<AuthenticatedDevice> provisioningServlet = new WebSocketResourceProviderFactory<>(
        provisioningEnvironment, AuthenticatedDevice.class, config.getWebSocketConfiguration(),
        RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);

    ServletRegistration.Dynamic websocket = environment.servlets().addServlet("WebSocket", webSocketServlet);
    ServletRegistration.Dynamic provisioning = environment.servlets().addServlet("Provisioning", provisioningServlet);

    websocket.addMapping(websocketServletPath);
    websocket.setAsyncSupported(true);

    provisioning.addMapping(provisioningWebsocketServletPath);
    provisioning.setAsyncSupported(true);

    environment.admin().addTask(new SetRequestLoggingEnabledTask());

    // healthcheck, admin port
    environment.healthChecks().register("cacheCluster", new RedisClusterHealthCheck(cacheCluster));

    MetricsUtil.registerSystemResourceMetrics(environment);
  }

  private void registerExceptionMappers(Environment environment,
      WebSocketEnvironment<AuthenticatedDevice> webSocketEnvironment,
      WebSocketEnvironment<AuthenticatedDevice> provisioningEnvironment) {

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
        new ObsoletePhoneNumberFormatExceptionMapper(),
        new RegistrationServiceSenderExceptionMapper(),
        new SubscriptionExceptionMapper(),
        new JsonMappingExceptionMapper()
    ).forEach(exceptionMapper -> {
      environment.jersey().register(exceptionMapper);
      webSocketEnvironment.jersey().register(exceptionMapper);
      provisioningEnvironment.jersey().register(exceptionMapper);
    });
  }

  public static void main(String[] args) throws Exception {
    new WhisperServerService().run(args);
  }
}
