/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.core.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.WhisperServerService.ExecutorServiceBuilder;
import org.whispersystems.textsecuregcm.WhisperServerService.ScheduledExecutorServiceBuilder;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupsDb;
import org.whispersystems.textsecuregcm.backup.Cdn3BackupCredentialGenerator;
import org.whispersystems.textsecuregcm.backup.Cdn3RemoteStorageManager;
import org.whispersystems.textsecuregcm.backup.SecureValueRecoveryBCredentialsGeneratorFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.controllers.SecureValueRecovery2Controller;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.experiment.PushNotificationExperimentSamples;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.MicrometerAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.FcmSender;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.AccountLockManager;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.DynamoDbRecoveryManager;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PagedSingleUseKEMPreKeyStore;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.RepeatedUseECSignedPreKeyStore;
import org.whispersystems.textsecuregcm.storage.RepeatedUseKEMSignedPreKeyStore;
import org.whispersystems.textsecuregcm.storage.ReportMessageDynamoDb;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.SingleUseECPreKeyStore;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager;
import org.whispersystems.textsecuregcm.storage.Subscriptions;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreClient;
import org.whispersystems.textsecuregcm.subscriptions.AppleAppStoreManager;
import org.whispersystems.textsecuregcm.subscriptions.GooglePlayBillingManager;
import org.whispersystems.textsecuregcm.util.ManagedAwsCrt;
import org.whispersystems.textsecuregcm.util.ManagedExecutors;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Construct utilities commonly used by worker commands
 */
public record CommandDependencies(
    AccountsManager accountsManager,
    ProfilesManager profilesManager,
    ReportMessageManager reportMessageManager,
    MessagesCache messagesCache,
    MessagesManager messagesManager,
    KeysManager keysManager,
    RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
    APNSender apnSender,
    FcmSender fcmSender,
    PushNotificationManager pushNotificationManager,
    PushNotificationExperimentSamples pushNotificationExperimentSamples,
    FaultTolerantRedisClusterClient cacheCluster,
    FaultTolerantRedisClusterClient pushSchedulerCluster,
    ClientResources.Builder redisClusterClientResourcesBuilder,
    BackupManager backupManager,
    IssuedReceiptsManager issuedReceiptsManager,
    GooglePlayBillingManager googlePlayBillingManager,
    AppleAppStoreManager appleAppStoreManager,
    SubscriptionManager subscriptionManager,
    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
    DynamoDbAsyncClient dynamoDbAsyncClient,
    PhoneNumberIdentifiers phoneNumberIdentifiers,
    DynamoDbRecoveryManager dynamoDbRecoveryManager) {

  static CommandDependencies build(
      final String name,
      final Environment environment,
      final WhisperServerConfiguration configuration)
      throws IOException, GeneralSecurityException, InvalidInputException {
    Clock clock = Clock.systemUTC();

    MetricsUtil.configureLogging(configuration, environment);

    environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    final AwsCredentialsProvider awsCredentialsProvider = configuration.getAwsCredentialsConfiguration().build();

    ScheduledExecutorService dynamicConfigurationExecutor = ScheduledExecutorServiceBuilder.of(environment, "dynamicConfiguration")
        .threads(1).build();

    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new DynamicConfigurationManager<>(
            configuration.getDynamicConfig().build(awsCredentialsProvider, dynamicConfigurationExecutor), DynamicConfiguration.class);
    dynamicConfigurationManager.start();
    ExperimentEnrollmentManager experimentEnrollmentManager =
        new ExperimentEnrollmentManager(dynamicConfigurationManager);

    final ClientResources.Builder redisClientResourcesBuilder = ClientResources.builder();

    FaultTolerantRedisClusterClient cacheCluster = configuration.getCacheClusterConfiguration()
        .build("main_cache", redisClientResourcesBuilder);
    FaultTolerantRedisClusterClient pushSchedulerCluster = configuration.getPushSchedulerCluster()
        .build("push_scheduler", redisClientResourcesBuilder);
    FaultTolerantRedisClient pubsubClient =
        configuration.getRedisPubSubConfiguration().build("pubsub", redisClientResourcesBuilder.build());

    Scheduler messageDeliveryScheduler = Schedulers.fromExecutorService(
        environment.lifecycle().executorService("messageDelivery").minThreads(4).maxThreads(4).build());
    ExecutorService messageDeletionExecutor = ExecutorServiceBuilder.of(environment, "messageDeletion")
        .minThreads(4).maxThreads(4).build();
    ExecutorService secureValueRecoveryServiceExecutor = ExecutorServiceBuilder.of(environment, "secureValueRecoveryService")
        .maxThreads(8).minThreads(8).build();
    ExecutorService storageServiceExecutor = ExecutorServiceBuilder.of(environment, "storageService")
        .maxThreads(8).minThreads(8).build();
    ExecutorService accountLockExecutor = ExecutorServiceBuilder.of(environment, "accountLock")
        .minThreads(8).maxThreads(8).build();
    ExecutorService remoteStorageHttpExecutor = ExecutorServiceBuilder.of(environment, "remoteStorage")

        .minThreads(0).maxThreads(Integer.MAX_VALUE).workQueue(new SynchronousQueue<>())
        .keepAliveTime(io.dropwizard.util.Duration.seconds(60L)).build();
    ExecutorService apnSenderExecutor = ExecutorServiceBuilder.of(environment, "apnSender")
        .maxThreads(1).minThreads(1).build();
    ExecutorService fcmSenderExecutor = ExecutorServiceBuilder.of(environment, "fcmSender")
        .maxThreads(16).minThreads(16).build();
    ExecutorService clientEventExecutor = ManagedExecutors.newVirtualThreadPerTaskExecutor(
      "clientEvent", configuration.getVirtualThreadConfiguration().maxConcurrentThreadsPerExecutor(), environment);
    ExecutorService asyncOperationQueueingExecutor = ExecutorServiceBuilder.of(environment, "asyncOperationQueueing")
        .minThreads(1).maxThreads(1).build();
    ExecutorService disconnectionRequestListenerExecutor = ManagedExecutors.newVirtualThreadPerTaskExecutor(
        "disconnectionRequest",
        configuration.getVirtualThreadConfiguration().maxConcurrentThreadsPerExecutor(),
        environment);

    final ScheduledExecutorService messagePollExecutor = ScheduledExecutorServiceBuilder.of(environment, "messagePollExecutor")
      .threads(1).build();
    final ScheduledExecutorService retryExecutor = ScheduledExecutorServiceBuilder.of(environment, "retry")
      .threads(1).build();

    ExternalServiceCredentialsGenerator storageCredentialsGenerator = SecureStorageController.credentialsGenerator(
        configuration.getSecureStorageServiceConfiguration());
    ExternalServiceCredentialsGenerator secureValueRecovery2CredentialsGenerator = SecureValueRecovery2Controller.credentialsGenerator(
        configuration.getSvr2Configuration());
    ExternalServiceCredentialsGenerator secureValueRecoveryBCredentialsGenerator =
        SecureValueRecoveryBCredentialsGeneratorFactory.svrbCredentialsGenerator(configuration.getSvrbConfiguration());

    final ExecutorService awsSdkMetricsExecutor = ManagedExecutors.newVirtualThreadPerTaskExecutor(
        "awsSdkMetrics",
        configuration.getVirtualThreadConfiguration().maxConcurrentThreadsPerExecutor(),
        environment);

    DynamoDbAsyncClient dynamoDbAsyncClient = configuration.getDynamoDbClientConfiguration()
        .buildAsyncClient(awsCredentialsProvider, new MicrometerAwsSdkMetricPublisher(awsSdkMetricsExecutor, "dynamoDbAsyncCommand"));

    DynamoDbClient dynamoDbClient = configuration.getDynamoDbClientConfiguration()
        .buildSyncClient(awsCredentialsProvider, new MicrometerAwsSdkMetricPublisher(awsSdkMetricsExecutor, "dynamoDbSyncCommand"));

    final AwsCredentialsProvider cdnCredentialsProvider = configuration.getCdnConfiguration().credentials().build();
    final S3AsyncClient asyncCdnS3Client = S3AsyncClient.builder()
        .credentialsProvider(cdnCredentialsProvider)
        .region(Region.of(configuration.getCdnConfiguration().region()))
        .build();


    RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        configuration.getDynamoDbTables().getRegistrationRecovery().getTableName(),
        configuration.getDynamoDbTables().getRegistrationRecovery().getExpiration(),
        dynamoDbAsyncClient,
        clock);

    Accounts accounts = new Accounts(
        clock,
        dynamoDbClient,
        dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getAccounts().getTableName(),
        configuration.getDynamoDbTables().getAccounts().getPhoneNumberTableName(),
        configuration.getDynamoDbTables().getAccounts().getPhoneNumberIdentifierTableName(),
        configuration.getDynamoDbTables().getAccounts().getUsernamesTableName(),
        configuration.getDynamoDbTables().getDeletedAccounts().getTableName(),
        configuration.getDynamoDbTables().getAccounts().getUsedLinkDeviceTokensTableName());
    PhoneNumberIdentifiers phoneNumberIdentifiers = new PhoneNumberIdentifiers(dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getPhoneNumberIdentifiers().getTableName());
    Profiles profiles = new Profiles(dynamoDbClient, dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getProfiles().getTableName());
    S3AsyncClient asyncKeysS3Client = S3AsyncClient.builder()
        .credentialsProvider(awsCredentialsProvider)
        .region(Region.of(configuration.getPagedSingleUseKEMPreKeyStore().region()))
        .build();
    PagedSingleUseKEMPreKeyStore pagedSingleUseKEMPreKeyStore = new PagedSingleUseKEMPreKeyStore(
        dynamoDbAsyncClient, asyncKeysS3Client,
        configuration.getDynamoDbTables().getPagedKemKeys().getTableName(),
        configuration.getPagedSingleUseKEMPreKeyStore().bucket());
    KeysManager keys = new KeysManager(
        new SingleUseECPreKeyStore(dynamoDbAsyncClient, configuration.getDynamoDbTables().getEcKeys().getTableName()),
        pagedSingleUseKEMPreKeyStore,
        new RepeatedUseECSignedPreKeyStore(dynamoDbAsyncClient,
            configuration.getDynamoDbTables().getEcSignedPreKeys().getTableName()),
        new RepeatedUseKEMSignedPreKeyStore(dynamoDbAsyncClient,
            configuration.getDynamoDbTables().getKemLastResortKeys().getTableName()));
    MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(dynamoDbClient, dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getMessages().getTableName(),
        configuration.getDynamoDbTables().getMessages().getExpiration(),
        messageDeletionExecutor, experimentEnrollmentManager);
    FaultTolerantRedisClusterClient messagesCluster = configuration.getMessageCacheConfiguration()
        .getRedisClusterConfiguration().build("messages", redisClientResourcesBuilder);
    FaultTolerantRedisClusterClient rateLimitersCluster = configuration.getRateLimitersCluster().build("rate_limiters",
        redisClientResourcesBuilder);
    SecureValueRecoveryClient secureValueRecovery2Client = new SecureValueRecoveryClient(
        secureValueRecovery2CredentialsGenerator,
        secureValueRecoveryServiceExecutor,
        retryExecutor,
        configuration.getSvr2Configuration(),
        () -> dynamicConfigurationManager.getConfiguration().getSvr2StatusCodesToIgnoreForAccountDeletion());
    SecureValueRecoveryClient secureValueRecoveryBClient = new SecureValueRecoveryClient(
        secureValueRecoveryBCredentialsGenerator,
        secureValueRecoveryServiceExecutor,
        retryExecutor,
        configuration.getSvrbConfiguration(),
        () -> dynamicConfigurationManager.getConfiguration().getSvrbStatusCodesToIgnoreForAccountDeletion());
    SecureStorageClient secureStorageClient = new SecureStorageClient(storageCredentialsGenerator,
        storageServiceExecutor, retryExecutor, configuration.getSecureStorageServiceConfiguration());
    DisconnectionRequestManager disconnectionRequestManager = new DisconnectionRequestManager(pubsubClient,
        disconnectionRequestListenerExecutor, retryExecutor);
    MessagesCache messagesCache = new MessagesCache(messagesCluster,
        messageDeliveryScheduler, messageDeletionExecutor, retryExecutor, Clock.systemUTC(), experimentEnrollmentManager);
    ProfilesManager profilesManager = new ProfilesManager(profiles, cacheCluster, retryExecutor, asyncCdnS3Client,
        configuration.getCdnConfiguration().bucket());
    ReportMessageDynamoDb reportMessageDynamoDb = new ReportMessageDynamoDb(dynamoDbClient, dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getReportMessage().getTableName(),
        configuration.getReportMessageConfiguration().getReportTtl());
    ReportMessageManager reportMessageManager = new ReportMessageManager(reportMessageDynamoDb, rateLimitersCluster,
        configuration.getReportMessageConfiguration().getCounterTtl());
    RedisMessageAvailabilityManager redisMessageAvailabilityManager =
        new RedisMessageAvailabilityManager(messagesCluster, clientEventExecutor, asyncOperationQueueingExecutor);
    MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager,
        reportMessageManager, messageDeletionExecutor, Clock.systemUTC());
    AccountLockManager accountLockManager = new AccountLockManager(dynamoDbClient,
        configuration.getDynamoDbTables().getDeletedAccountsLock().getTableName());
    RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords);
    AccountsManager accountsManager = new AccountsManager(accounts, phoneNumberIdentifiers, cacheCluster,
        pubsubClient, accountLockManager, keys, messagesManager, profilesManager,
        secureStorageClient, secureValueRecovery2Client, disconnectionRequestManager,
        registrationRecoveryPasswordsManager, accountLockExecutor, messagePollExecutor,
        retryExecutor, clock, configuration.getLinkDeviceSecretConfiguration().secret().value());
    RateLimiters rateLimiters = RateLimiters.create(dynamicConfigurationManager, rateLimitersCluster, retryExecutor);
    final BackupsDb backupsDb =
        new BackupsDb(dynamoDbAsyncClient, configuration.getDynamoDbTables().getBackups().getTableName(), clock);
    final GenericServerSecretParams backupsGenericZkSecretParams;
    try {
      backupsGenericZkSecretParams =
          new GenericServerSecretParams(configuration.getBackupsZkConfig().serverSecret().value());
    } catch (InvalidInputException e) {
      throw new IllegalArgumentException(e);
    }
    final BackupManager backupManager = new BackupManager(
        backupsDb,
        backupsGenericZkSecretParams,
        rateLimiters,
        new TusAttachmentGenerator(configuration.getTus()),
        new Cdn3BackupCredentialGenerator(configuration.getTus()),
        new Cdn3RemoteStorageManager(
            remoteStorageHttpExecutor,
            retryExecutor,
            configuration.getCdn3StorageManagerConfiguration()),
        secureValueRecoveryBCredentialsGenerator,
        secureValueRecoveryBClient,
        clock,
        dynamicConfigurationManager);

    final IssuedReceiptsManager issuedReceiptsManager = new IssuedReceiptsManager(
        configuration.getDynamoDbTables().getIssuedReceipts().getTableName(),
        configuration.getDynamoDbTables().getIssuedReceipts().getExpiration(),
        dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getIssuedReceipts().getGenerator(),
        configuration.getDynamoDbTables().getIssuedReceipts().getmaxIssuedReceiptsPerPaymentId());

    final ServerSecretParams zkSecretParams = new ServerSecretParams(configuration.getZkConfig().serverSecret().value());
    final ServerZkReceiptOperations zkReceiptOperations = new ServerZkReceiptOperations(zkSecretParams);
    GooglePlayBillingManager googlePlayBillingManager = new GooglePlayBillingManager(
        new ByteArrayInputStream(configuration.getGooglePlayBilling().credentialsJson().getBytes(StandardCharsets.UTF_8)),
        configuration.getGooglePlayBilling().packageName(),
        configuration.getGooglePlayBilling().applicationName(),
        configuration.getGooglePlayBilling().productIdToLevel());
    AppleAppStoreManager appleAppStoreManager = new AppleAppStoreManager(
        new AppleAppStoreClient(
            configuration.getAppleAppStore().env(),
            configuration.getAppleAppStore().bundleId(),
            configuration.getAppleAppStore().appAppleId(),
            configuration.getAppleAppStore().issuerId(),
            configuration.getAppleAppStore().keyId(),
            configuration.getAppleAppStore().encodedKey().value(),
            configuration.getAppleAppStore().appleRootCerts(),
            configuration.getAppleAppStore().retryConfigurationName()),
        configuration.getAppleAppStore().subscriptionGroupId(),
        configuration.getAppleAppStore().productIdToLevel());
    final SubscriptionManager subscriptionManager = new SubscriptionManager(
        new Subscriptions(configuration.getDynamoDbTables().getSubscriptions().getTableName(), dynamoDbAsyncClient),
        List.of(googlePlayBillingManager, appleAppStoreManager),
        zkReceiptOperations,
        issuedReceiptsManager);

    APNSender apnSender = new APNSender(apnSenderExecutor, configuration.getApnConfiguration());
    FcmSender fcmSender = new FcmSender(fcmSenderExecutor, configuration.getFcmConfiguration().credentials().value());
    PushNotificationScheduler pushNotificationScheduler = new PushNotificationScheduler(pushSchedulerCluster,
        apnSender, fcmSender, accountsManager, 0, 0, retryExecutor);
    PushNotificationManager pushNotificationManager = new PushNotificationManager(accountsManager,
        apnSender, fcmSender, pushNotificationScheduler);
    PushNotificationExperimentSamples pushNotificationExperimentSamples =
        new PushNotificationExperimentSamples(dynamoDbAsyncClient,
            configuration.getDynamoDbTables().getPushNotificationExperimentSamples().getTableName(),
            Clock.systemUTC());

    final DynamoDbRecoveryManager dynamoDbRecoveryManager =
        new DynamoDbRecoveryManager(accounts, phoneNumberIdentifiers);

    environment.lifecycle().manage(apnSender);
    environment.lifecycle().manage(disconnectionRequestManager);
    environment.lifecycle().manage(redisMessageAvailabilityManager);
    environment.lifecycle().manage(new ManagedAwsCrt());

    return new CommandDependencies(
        accountsManager,
        profilesManager,
        reportMessageManager,
        messagesCache,
        messagesManager,
        keys,
        registrationRecoveryPasswordsManager,
        apnSender,
        fcmSender,
        pushNotificationManager,
        pushNotificationExperimentSamples,
        cacheCluster,
        pushSchedulerCluster,
        redisClientResourcesBuilder,
        backupManager,
        issuedReceiptsManager,
        googlePlayBillingManager,
        appleAppStoreManager,
        subscriptionManager,
        dynamicConfigurationManager,
        dynamoDbAsyncClient,
        phoneNumberIdentifiers,
        dynamoDbRecoveryManager
    );
  }

}
