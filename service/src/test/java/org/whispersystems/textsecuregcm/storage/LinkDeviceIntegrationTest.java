package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.Pair;

public class LinkDeviceIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.ACCOUNTS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK,
      DynamoDbExtensionSchema.Tables.NUMBERS,
      DynamoDbExtensionSchema.Tables.PNI,
      DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS,
      DynamoDbExtensionSchema.Tables.USERNAMES,
      DynamoDbExtensionSchema.Tables.EC_KEYS,
      DynamoDbExtensionSchema.Tables.PQ_KEYS,
      DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS);

  @RegisterExtension
  static final RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static final Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private ExecutorService accountLockExecutor;
  private ExecutorService clientPresenceExecutor;

  private AccountsManager accountsManager;
  private KeysManager keysManager;

  @BeforeEach
  void setUp() {
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    keysManager = new KeysManager(
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.EC_KEYS.tableName(),
        DynamoDbExtensionSchema.Tables.PQ_KEYS.tableName(),
        DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName(),
        DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName(),
        dynamicConfigurationManager);

    final Accounts accounts = new Accounts(
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.NUMBERS.tableName(),
        DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS.tableName(),
        DynamoDbExtensionSchema.Tables.USERNAMES.tableName(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS.tableName());

    accountLockExecutor = Executors.newSingleThreadExecutor();
    clientPresenceExecutor = Executors.newSingleThreadExecutor();

    final AccountLockManager accountLockManager = new AccountLockManager(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK.tableName());

    final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
    when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureValueRecovery2Client svr2Client = mock(SecureValueRecovery2Client.class);
    when(svr2Client.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
            DynamoDbExtensionSchema.Tables.PNI.tableName());

    final MessagesManager messagesManager = mock(MessagesManager.class);
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    final ProfilesManager profilesManager = mock(ProfilesManager.class);
    when(profilesManager.deleteAll(any())).thenReturn(CompletableFuture.completedFuture(null));

    final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        mock(RegistrationRecoveryPasswordsManager.class);

    when(registrationRecoveryPasswordsManager.removeForNumber(any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        CACHE_CLUSTER_EXTENSION.getRedisCluster(),
        accountLockManager,
        keysManager,
        messagesManager,
        profilesManager,
        secureStorageClient,
        svr2Client,
        mock(ClientPresenceManager.class),
        mock(ExperimentEnrollmentManager.class),
        registrationRecoveryPasswordsManager,
        accountLockExecutor,
        clientPresenceExecutor,
        CLOCK);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    accountLockExecutor.shutdown();
    clientPresenceExecutor.shutdown();

    //noinspection ResultOfMethodCallIgnored
    accountLockExecutor.awaitTermination(1, TimeUnit.SECONDS);

    //noinspection ResultOfMethodCallIgnored
    clientPresenceExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void linkDevice() throws InterruptedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final Account account = AccountsHelper.createAccount(accountsManager, number);
    assertEquals(1, accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getDevices().size());

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account, new DeviceSpec(
                "device-name".getBytes(StandardCharsets.UTF_8),
                "password",
                "OWT",
                new Device.DeviceCapabilities(true, true, true, true),
                1,
                2,
                true,
                Optional.empty(),
                Optional.empty(),
                KeysHelper.signedECPreKey(1, aciKeyPair),
                KeysHelper.signedECPreKey(2, pniKeyPair),
                KeysHelper.signedKEMPreKey(3, aciKeyPair),
                KeysHelper.signedKEMPreKey(4, pniKeyPair)))
            .join();

    assertEquals(2, updatedAccountAndDevice.first().getDevices().size());

    assertEquals(2,
        accountsManager.getByAccountIdentifier(updatedAccountAndDevice.first().getUuid()).orElseThrow().getDevices()
            .size());
  }
}
