package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.redis.RedisServerExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.Pair;

public class AddRemoveDeviceIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.ACCOUNTS,
      DynamoDbExtensionSchema.Tables.CLIENT_PUBLIC_KEYS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK,
      DynamoDbExtensionSchema.Tables.USED_LINK_DEVICE_TOKENS,
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

  @RegisterExtension
  static final RedisServerExtension PUBSUB_SERVER_EXTENSION = RedisServerExtension.builder().build();

  private static final Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private ExecutorService accountLockExecutor;
  private ExecutorService clientPresenceExecutor;

  private KeysManager keysManager;
  private ClientPublicKeysManager clientPublicKeysManager;
  private MessagesManager messagesManager;
  private AccountsManager accountsManager;

  @BeforeEach
  void setUp() {
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    keysManager = new KeysManager(
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.EC_KEYS.tableName(),
        DynamoDbExtensionSchema.Tables.PQ_KEYS.tableName(),
        DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName(),
        DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName()
    );

    final ClientPublicKeys clientPublicKeys = new ClientPublicKeys(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.CLIENT_PUBLIC_KEYS.tableName());

    final Accounts accounts = new Accounts(
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.NUMBERS.tableName(),
        DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS.tableName(),
        DynamoDbExtensionSchema.Tables.USERNAMES.tableName(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.USED_LINK_DEVICE_TOKENS.tableName());

    accountLockExecutor = Executors.newSingleThreadExecutor();
    clientPresenceExecutor = Executors.newSingleThreadExecutor();

    final AccountLockManager accountLockManager = new AccountLockManager(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK.tableName());

    clientPublicKeysManager = new ClientPublicKeysManager(clientPublicKeys, accountLockManager, accountLockExecutor);

    final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
    when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureValueRecovery2Client svr2Client = mock(SecureValueRecovery2Client.class);
    when(svr2Client.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
            DynamoDbExtensionSchema.Tables.PNI.tableName());

    messagesManager = mock(MessagesManager.class);
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    final ProfilesManager profilesManager = mock(ProfilesManager.class);
    when(profilesManager.deleteAll(any())).thenReturn(CompletableFuture.completedFuture(null));

    final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        mock(RegistrationRecoveryPasswordsManager.class);

    when(registrationRecoveryPasswordsManager.removeForNumber(any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    PUBSUB_SERVER_EXTENSION.getRedisClient().useConnection(connection -> {
      connection.sync().flushall();
      connection.sync().configSet("notify-keyspace-events", "K$");
    });

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        CACHE_CLUSTER_EXTENSION.getRedisCluster(),
        PUBSUB_SERVER_EXTENSION.getRedisClient(),
        accountLockManager,
        keysManager,
        messagesManager,
        profilesManager,
        secureStorageClient,
        svr2Client,
        mock(ClientPresenceManager.class),
        registrationRecoveryPasswordsManager,
        clientPublicKeysManager,
        accountLockExecutor,
        clientPresenceExecutor,
        CLOCK,
        "link-device-secret".getBytes(StandardCharsets.UTF_8),
        dynamicConfigurationManager);

    accountsManager.start();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    accountsManager.stop();

    accountLockExecutor.shutdown();
    clientPresenceExecutor.shutdown();

    //noinspection ResultOfMethodCallIgnored
    accountLockExecutor.awaitTermination(1, TimeUnit.SECONDS);

    //noinspection ResultOfMethodCallIgnored
    clientPresenceExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void addDevice() throws InterruptedException {
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
                new Device.DeviceCapabilities(true, true, false, false),
                1,
                2,
                true,
                Optional.empty(),
                Optional.empty(),
                KeysHelper.signedECPreKey(1, aciKeyPair),
                KeysHelper.signedECPreKey(2, pniKeyPair),
                KeysHelper.signedKEMPreKey(3, aciKeyPair),
                KeysHelper.signedKEMPreKey(4, pniKeyPair)),
                accountsManager.generateLinkDeviceToken(account.getIdentifier(IdentityType.ACI)))
            .join();

    assertEquals(2, updatedAccountAndDevice.first().getDevices().size());

    assertEquals(2,
        accountsManager.getByAccountIdentifier(updatedAccountAndDevice.first().getUuid()).orElseThrow().getDevices()
            .size());

    final byte addedDeviceId = updatedAccountAndDevice.second().getId();

    assertTrue(keysManager.getEcSignedPreKey(updatedAccountAndDevice.first().getUuid(), addedDeviceId).join().isPresent());
    assertTrue(keysManager.getEcSignedPreKey(updatedAccountAndDevice.first().getPhoneNumberIdentifier(), addedDeviceId).join().isPresent());
    assertTrue(keysManager.getLastResort(updatedAccountAndDevice.first().getUuid(), addedDeviceId).join().isPresent());
    assertTrue(keysManager.getLastResort(updatedAccountAndDevice.first().getPhoneNumberIdentifier(), addedDeviceId).join().isPresent());
  }

  @Test
  void addDeviceReusedToken() throws InterruptedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final Account account = AccountsHelper.createAccount(accountsManager, number);
    assertEquals(1, accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getDevices().size());

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(account.getIdentifier(IdentityType.ACI));

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account, new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    new Device.DeviceCapabilities(true, true, false, false),
                    1,
                    2,
                    true,
                    Optional.empty(),
                    Optional.empty(),
                    KeysHelper.signedECPreKey(1, aciKeyPair),
                    KeysHelper.signedECPreKey(2, pniKeyPair),
                    KeysHelper.signedKEMPreKey(3, aciKeyPair),
                    KeysHelper.signedKEMPreKey(4, pniKeyPair)),
                linkDeviceToken)
            .join();

    assertEquals(2,
        accountsManager.getByAccountIdentifier(updatedAccountAndDevice.first().getUuid()).orElseThrow().getDevices()
            .size());

    final CompletionException completionException = assertThrows(CompletionException.class,
        () -> accountsManager.addDevice(account, new DeviceSpec(
                "device-name".getBytes(StandardCharsets.UTF_8),
                "password",
                "OWT",
                new Device.DeviceCapabilities(true, true, false, false),
                1,
                2,
                true,
                Optional.empty(),
                Optional.empty(),
                KeysHelper.signedECPreKey(1, aciKeyPair),
                KeysHelper.signedECPreKey(2, pniKeyPair),
                KeysHelper.signedKEMPreKey(3, aciKeyPair),
                KeysHelper.signedKEMPreKey(4, pniKeyPair)),
            linkDeviceToken)
        .join());

    assertInstanceOf(LinkDeviceTokenAlreadyUsedException.class, completionException.getCause());

    assertEquals(2,
        accountsManager.getByAccountIdentifier(updatedAccountAndDevice.first().getUuid()).orElseThrow().getDevices()
            .size());
  }

  @Test
  void removeDevice() throws InterruptedException {
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
                new Device.DeviceCapabilities(true, true, false, false),
                1,
                2,
                true,
                Optional.empty(),
                Optional.empty(),
                KeysHelper.signedECPreKey(1, aciKeyPair),
                KeysHelper.signedECPreKey(2, pniKeyPair),
                KeysHelper.signedKEMPreKey(3, aciKeyPair),
                KeysHelper.signedKEMPreKey(4, pniKeyPair)),
                accountsManager.generateLinkDeviceToken(account.getIdentifier(IdentityType.ACI)))
            .join();

    final byte addedDeviceId = updatedAccountAndDevice.second().getId();

    clientPublicKeysManager.setPublicKey(account, Device.PRIMARY_ID, Curve.generateKeyPair().getPublicKey()).join();
    clientPublicKeysManager.setPublicKey(account, addedDeviceId, Curve.generateKeyPair().getPublicKey()).join();

    final Account updatedAccount = accountsManager.removeDevice(updatedAccountAndDevice.first(), addedDeviceId).join();

    assertEquals(1, updatedAccount.getDevices().size());

    assertFalse(keysManager.getEcSignedPreKey(updatedAccount.getUuid(), addedDeviceId).join().isPresent());
    assertFalse(keysManager.getEcSignedPreKey(updatedAccount.getPhoneNumberIdentifier(), addedDeviceId).join().isPresent());
    assertFalse(keysManager.getLastResort(updatedAccount.getUuid(), addedDeviceId).join().isPresent());
    assertFalse(keysManager.getLastResort(updatedAccount.getPhoneNumberIdentifier(), addedDeviceId).join().isPresent());
    assertFalse(clientPublicKeysManager.findPublicKey(updatedAccount.getUuid(), addedDeviceId).join().isPresent());

    assertTrue(keysManager.getEcSignedPreKey(updatedAccount.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getEcSignedPreKey(updatedAccount.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(updatedAccount.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(updatedAccount.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(clientPublicKeysManager.findPublicKey(updatedAccount.getUuid(), Device.PRIMARY_ID).join().isPresent());
  }

  @Test
  void removeDevicePartialFailure() throws InterruptedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final Account account = AccountsHelper.createAccount(accountsManager, number);
    assertEquals(1, accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getDevices().size());

    final UUID aci = account.getIdentifier(IdentityType.ACI);

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account, new DeviceSpec(
                "device-name".getBytes(StandardCharsets.UTF_8),
                "password",
                "OWT",
                new Device.DeviceCapabilities(true, true, false, false),
                1,
                2,
                true,
                Optional.empty(),
                Optional.empty(),
                KeysHelper.signedECPreKey(1, aciKeyPair),
                KeysHelper.signedECPreKey(2, pniKeyPair),
                KeysHelper.signedKEMPreKey(3, aciKeyPair),
                KeysHelper.signedKEMPreKey(4, pniKeyPair)),
                accountsManager.generateLinkDeviceToken(account.getIdentifier(IdentityType.ACI)))
            .join();

    final byte addedDeviceId = updatedAccountAndDevice.second().getId();

    when(messagesManager.clear(any(), anyByte()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("OH NO")));

    assertThrows(CompletionException.class,
        () -> accountsManager.removeDevice(updatedAccountAndDevice.first(), addedDeviceId).join());

    final Account retrievedAccount = accountsManager.getByAccountIdentifierAsync(aci).join().orElseThrow();

    clientPublicKeysManager.setPublicKey(retrievedAccount, Device.PRIMARY_ID, Curve.generateKeyPair().getPublicKey()).join();
    clientPublicKeysManager.setPublicKey(retrievedAccount, addedDeviceId, Curve.generateKeyPair().getPublicKey()).join();

    assertEquals(2, retrievedAccount.getDevices().size());

    assertTrue(keysManager.getEcSignedPreKey(retrievedAccount.getUuid(), addedDeviceId).join().isPresent());
    assertTrue(keysManager.getEcSignedPreKey(retrievedAccount.getPhoneNumberIdentifier(), addedDeviceId).join().isPresent());
    assertTrue(keysManager.getLastResort(retrievedAccount.getUuid(), addedDeviceId).join().isPresent());
    assertTrue(keysManager.getLastResort(retrievedAccount.getPhoneNumberIdentifier(), addedDeviceId).join().isPresent());
    assertTrue(clientPublicKeysManager.findPublicKey(retrievedAccount.getUuid(), addedDeviceId).join().isPresent());

    assertTrue(keysManager.getEcSignedPreKey(retrievedAccount.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getEcSignedPreKey(retrievedAccount.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(retrievedAccount.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(retrievedAccount.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(clientPublicKeysManager.findPublicKey(retrievedAccount.getUuid(), Device.PRIMARY_ID).join().isPresent());
  }

  @Test
  void waitForNewLinkedDevice() throws InterruptedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final Account account = AccountsHelper.createAccount(accountsManager, number);

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(account.getIdentifier(IdentityType.ACI));
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    final CompletableFuture<Optional<DeviceInfo>> displacedFuture =
        accountsManager.waitForNewLinkedDevice(linkDeviceTokenIdentifier, Duration.ofSeconds(5));

    final CompletableFuture<Optional<DeviceInfo>> activeFuture =
        accountsManager.waitForNewLinkedDevice(linkDeviceTokenIdentifier, Duration.ofSeconds(5));

    assertEquals(Optional.empty(), displacedFuture.join());

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account, new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    new Device.DeviceCapabilities(true, true, true, false),
                    1,
                    2,
                    true,
                    Optional.empty(),
                    Optional.empty(),
                    KeysHelper.signedECPreKey(1, aciKeyPair),
                    KeysHelper.signedECPreKey(2, pniKeyPair),
                    KeysHelper.signedKEMPreKey(3, aciKeyPair),
                    KeysHelper.signedKEMPreKey(4, pniKeyPair)),
                linkDeviceToken)
            .join();

    final Optional<DeviceInfo> maybeDeviceInfo = activeFuture.join();

    assertTrue(maybeDeviceInfo.isPresent());
    final DeviceInfo deviceInfo = maybeDeviceInfo.get();

    assertEquals(updatedAccountAndDevice.second().getId(), deviceInfo.id());
    assertEquals(updatedAccountAndDevice.second().getCreated(), deviceInfo.created());
  }

  @Test
  void waitForNewLinkedDeviceAlreadyAdded() throws InterruptedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final Account account = AccountsHelper.createAccount(accountsManager, number);

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(account.getIdentifier(IdentityType.ACI));
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account, new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    new Device.DeviceCapabilities(true, true, true, false),
                    1,
                    2,
                    true,
                    Optional.empty(),
                    Optional.empty(),
                    KeysHelper.signedECPreKey(1, aciKeyPair),
                    KeysHelper.signedECPreKey(2, pniKeyPair),
                    KeysHelper.signedKEMPreKey(3, aciKeyPair),
                    KeysHelper.signedKEMPreKey(4, pniKeyPair)),
                linkDeviceToken)
            .join();

    final CompletableFuture<Optional<DeviceInfo>> linkedDeviceFuture =
        accountsManager.waitForNewLinkedDevice(linkDeviceTokenIdentifier, Duration.ofMinutes(1));

    final Optional<DeviceInfo> maybeDeviceInfo = linkedDeviceFuture.join();

    assertTrue(maybeDeviceInfo.isPresent());
    final DeviceInfo deviceInfo = maybeDeviceInfo.get();

    assertEquals(updatedAccountAndDevice.second().getId(), deviceInfo.id());
    assertEquals(updatedAccountAndDevice.second().getCreated(), deviceInfo.created());
  }

  @Test
  void waitForNewLinkedDeviceTimeout() {
    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(UUID.randomUUID());
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    final CompletableFuture<Optional<DeviceInfo>> linkedDeviceFuture =
        accountsManager.waitForNewLinkedDevice(linkDeviceTokenIdentifier, Duration.ofMillis(10));

    final Optional<DeviceInfo> maybeDeviceInfo = linkedDeviceFuture.join();

    assertTrue(maybeDeviceInfo.isEmpty());
  }
}
