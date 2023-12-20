/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static com.codahale.metrics.MetricRegistry.name;

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.core.Application;
import io.dropwizard.core.cli.EnvironmentCommand;
import io.dropwizard.core.setup.Environment;
import io.lettuce.core.resource.ClientResources;
import java.time.Clock;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.WhisperServerService;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.SecureStorageController;
import org.whispersystems.textsecuregcm.controllers.SecureValueRecovery2Controller;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountLockManager;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageDynamoDb;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class AssignUsernameCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  public AssignUsernameCommand() {
    super(new Application<>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment) {

      }
    }, "assign-username-hash", "assign a username hash to an account");
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-u", "--usernameHash")
        .dest("usernameHash")
        .type(String.class)
        .required(true)
        .help("The username hash to assign");

    subparser.addArgument("-e", "--encryptedUsername")
        .dest("encryptedUsername")
        .type(String.class)
        .required(false)
        .help("The encrypted username for the username link");

    subparser.addArgument("-a", "--aci")
        .dest("aci")
        .type(String.class)
        .required(true)
        .help("The ACI of the account to which to assign the username");
  }

  @Override
  protected void run(Environment environment, Namespace namespace,
      WhisperServerConfiguration configuration)
      throws Exception {
    environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    ClientResources redisClusterClientResources = ClientResources.builder().build();

    FaultTolerantRedisCluster cacheCluster = new FaultTolerantRedisCluster("main_cache_cluster",
        configuration.getCacheClusterConfiguration(), redisClusterClientResources);

    Scheduler messageDeliveryScheduler = Schedulers.fromExecutorService(
        environment.lifecycle().executorService("messageDelivery-%d").maxThreads(4)
            .build());
    ExecutorService keyspaceNotificationDispatchExecutor = environment.lifecycle()
        .executorService(name(getClass(), "keyspaceNotification-%d")).maxThreads(4).build();
    ExecutorService messageDeletionExecutor = environment.lifecycle()
        .executorService(name(getClass(), "messageDeletion-%d")).maxThreads(4).build();
    ExecutorService secureValueRecoveryExecutor = environment.lifecycle()
        .executorService(name(getClass(), "secureValueRecoveryService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService storageServiceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "storageService-%d")).maxThreads(1).minThreads(1).build();
    ExecutorService accountLockExecutor = environment.lifecycle()
        .executorService(name(getClass(), "accountLock-%d")).minThreads(1).maxThreads(1).build();
    ExecutorService clientPresenceExecutor = environment.lifecycle()
        .executorService(name(getClass(), "clientPresence-%d")).minThreads(1).maxThreads(1).build();
    ScheduledExecutorService secureValueRecoveryServiceRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "secureValueRecoveryServiceRetry-%d")).threads(1).build();
    ScheduledExecutorService storageServiceRetryExecutor = environment.lifecycle()
        .scheduledExecutorService(name(getClass(), "storageServiceRetry-%d")).threads(1).build();

    ExternalServiceCredentialsGenerator storageCredentialsGenerator = SecureStorageController.credentialsGenerator(
        configuration.getSecureStorageServiceConfiguration());
    ExternalServiceCredentialsGenerator secureValueRecoveryCredentialsGenerator = SecureValueRecovery2Controller.credentialsGenerator(
        configuration.getSvr2Configuration());

    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new DynamicConfigurationManager<>(
        configuration.getAppConfig().getApplication(), configuration.getAppConfig().getEnvironment(),
        configuration.getAppConfig().getConfigurationName(), DynamicConfiguration.class);
    dynamicConfigurationManager.start();

    ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(
        dynamicConfigurationManager);

    DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbFromConfig.asyncClient(
        configuration.getDynamoDbClientConfiguration(), WhisperServerService.AWSSDK_CREDENTIALS_PROVIDER);

    DynamoDbClient dynamoDbClient = DynamoDbFromConfig.client(configuration.getDynamoDbClientConfiguration(),
        WhisperServerService.AWSSDK_CREDENTIALS_PROVIDER);

    RegistrationRecoveryPasswords registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        configuration.getDynamoDbTables().getRegistrationRecovery().getTableName(),
        configuration.getDynamoDbTables().getRegistrationRecovery().getExpiration(),
        dynamoDbClient,
        dynamoDbAsyncClient
    );

    RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords);

    Accounts accounts = new Accounts(
        dynamoDbClient,
        dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getAccounts().getTableName(),
        configuration.getDynamoDbTables().getAccounts().getPhoneNumberTableName(),
        configuration.getDynamoDbTables().getAccounts().getPhoneNumberIdentifierTableName(),
        configuration.getDynamoDbTables().getAccounts().getUsernamesTableName(),
        configuration.getDynamoDbTables().getDeletedAccounts().getTableName());
    PhoneNumberIdentifiers phoneNumberIdentifiers = new PhoneNumberIdentifiers(dynamoDbClient,
        configuration.getDynamoDbTables().getPhoneNumberIdentifiers().getTableName());
    Profiles profiles = new Profiles(dynamoDbClient, dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getProfiles().getTableName());
    KeysManager keys = new KeysManager(
            dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getEcKeys().getTableName(),
        configuration.getDynamoDbTables().getKemKeys().getTableName(),
        configuration.getDynamoDbTables().getEcSignedPreKeys().getTableName(),
        configuration.getDynamoDbTables().getKemLastResortKeys().getTableName()
    );
    MessagesDynamoDb messagesDynamoDb = new MessagesDynamoDb(dynamoDbClient, dynamoDbAsyncClient,
        configuration.getDynamoDbTables().getMessages().getTableName(),
        configuration.getDynamoDbTables().getMessages().getExpiration(),
        messageDeletionExecutor);
    FaultTolerantRedisCluster messageInsertCacheCluster = new FaultTolerantRedisCluster("message_insert_cluster",
        configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
    FaultTolerantRedisCluster messageReadDeleteCluster = new FaultTolerantRedisCluster("message_read_delete_cluster",
        configuration.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
    FaultTolerantRedisCluster clientPresenceCluster = new FaultTolerantRedisCluster("client_presence_cluster",
        configuration.getClientPresenceClusterConfiguration(), redisClusterClientResources);
    FaultTolerantRedisCluster rateLimitersCluster = new FaultTolerantRedisCluster("rate_limiters",
        configuration.getRateLimitersCluster(), redisClusterClientResources);
    SecureValueRecovery2Client secureValueRecovery2Client = new SecureValueRecovery2Client(
        secureValueRecoveryCredentialsGenerator, secureValueRecoveryExecutor, secureValueRecoveryServiceRetryExecutor,
        configuration.getSvr2Configuration());
    SecureStorageClient secureStorageClient = new SecureStorageClient(storageCredentialsGenerator,
        storageServiceExecutor, storageServiceRetryExecutor, configuration.getSecureStorageServiceConfiguration());
    ClientPresenceManager clientPresenceManager = new ClientPresenceManager(clientPresenceCluster,
        Executors.newSingleThreadScheduledExecutor(), keyspaceNotificationDispatchExecutor);
    MessagesCache messagesCache = new MessagesCache(messageInsertCacheCluster, messageReadDeleteCluster,
        keyspaceNotificationDispatchExecutor, messageDeliveryScheduler, messageDeletionExecutor, Clock.systemUTC());
    ProfilesManager profilesManager = new ProfilesManager(profiles, cacheCluster);
    ReportMessageDynamoDb reportMessageDynamoDb = new ReportMessageDynamoDb(dynamoDbClient,
        configuration.getDynamoDbTables().getReportMessage().getTableName(),
        configuration.getReportMessageConfiguration().getReportTtl());
    ReportMessageManager reportMessageManager = new ReportMessageManager(reportMessageDynamoDb, rateLimitersCluster,
        configuration.getReportMessageConfiguration().getCounterTtl());
    MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache,
        reportMessageManager, messageDeletionExecutor);
    AccountLockManager accountLockManager = new AccountLockManager(dynamoDbClient,
        configuration.getDynamoDbTables().getDeletedAccountsLock().getTableName());
    AccountsManager accountsManager = new AccountsManager(accounts, phoneNumberIdentifiers, cacheCluster,
        accountLockManager, keys, messagesManager, profilesManager,
            secureStorageClient, secureValueRecovery2Client, clientPresenceManager,
        experimentEnrollmentManager, registrationRecoveryPasswordsManager, accountLockExecutor, clientPresenceExecutor,
        Clock.systemUTC());

    final String usernameHash = namespace.getString("usernameHash");
    final String encryptedUsername = namespace.getString("encryptedUsername");
    final UUID accountIdentifier = UUID.fromString(namespace.getString("aci"));

    accountsManager.getByAccountIdentifier(accountIdentifier).ifPresentOrElse(account -> {
          try {
            final AccountsManager.UsernameReservation reservation = accountsManager.reserveUsernameHash(account,
                List.of(Base64.getUrlDecoder().decode(usernameHash))).join();
            final Account result = accountsManager.confirmReservedUsernameHash(
                account,
                reservation.reservedUsernameHash(),
                encryptedUsername == null ? null : Base64.getUrlDecoder().decode(encryptedUsername)).join();
            System.out.println("New username hash: " + Base64.getUrlEncoder().encodeToString(result.getUsernameHash().orElseThrow()));
            System.out.println("New username link handle: " + result.getUsernameLinkHandle().toString());
          } catch (final CompletionException e) {
            if (ExceptionUtils.unwrap(e) instanceof UsernameHashNotAvailableException) {
              throw new IllegalArgumentException("Username hash already taken");
            }

            if (ExceptionUtils.unwrap(e) instanceof UsernameReservationNotFoundException) {
              throw new IllegalArgumentException("Username hash reservation not found");
            }

            throw e;
          }
        },
        () -> {
          throw new IllegalArgumentException("Account not found");
        });
  }
}
