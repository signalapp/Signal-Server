package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.redis.RedisServerExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.TestClock;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class AddRemoveDeviceIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.ACCOUNTS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK,
      DynamoDbExtensionSchema.Tables.USED_LINK_DEVICE_TOKENS,
      DynamoDbExtensionSchema.Tables.NUMBERS,
      DynamoDbExtensionSchema.Tables.PNI,
      DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS,
      DynamoDbExtensionSchema.Tables.USERNAMES,
      DynamoDbExtensionSchema.Tables.EC_KEYS,
      DynamoDbExtensionSchema.Tables.PAGED_PQ_KEYS,
      DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS,
      DynamoDbExtensionSchema.Tables.PHONE_NUMBER_RECOVERY_PASSWORDS);

  @RegisterExtension
  static final RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @RegisterExtension
  static final RedisServerExtension PUBSUB_SERVER_EXTENSION = RedisServerExtension.builder().build();

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension("testbucket");

  private ScheduledExecutorService scheduledExecutorService;

  private KeysManager keysManager;
  private MessagesManager messagesManager;
  private AccountsManager accountsManager;
  private TestClock clock;

  @BeforeEach
  void setUp() {
    clock = TestClock.pinned(Instant.now());

    final DynamoDbAsyncClient dynamoDbAsyncClient = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient();
    keysManager = new KeysManager(
        new SingleUseECPreKeyStore(dynamoDbAsyncClient, DynamoDbExtensionSchema.Tables.EC_KEYS.tableName()),
        new PagedSingleUseKEMPreKeyStore(dynamoDbAsyncClient,
            S3_EXTENSION.getS3Client(),
            DynamoDbExtensionSchema.Tables.PAGED_PQ_KEYS.tableName(),
            S3_EXTENSION.getBucketName()),
        new RepeatedUseECSignedPreKeyStore(dynamoDbAsyncClient,
            DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName()),
        new RepeatedUseKEMSignedPreKeyStore(dynamoDbAsyncClient,
            DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName()));

    final Accounts accounts = new Accounts(
        clock,
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        new RedeemedReceiptsManager(clock, DynamoDbExtensionSchema.Tables.REDEEMED_RECEIPTS.tableName(),
            DYNAMO_DB_EXTENSION.getDynamoDbClient()),
        DynamoDbExtensionSchema.Tables.ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.NUMBERS.tableName(),
        DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS.tableName(),
        DynamoDbExtensionSchema.Tables.USERNAMES.tableName(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.USED_LINK_DEVICE_TOKENS.tableName());

    scheduledExecutorService = mock(ScheduledExecutorService.class);

    final AccountLockManager accountLockManager = new AccountLockManager(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK.tableName());

    final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
    when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureValueRecoveryClient svr2Client = mock(SecureValueRecoveryClient.class);
    when(svr2Client.removeData(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
            DynamoDbExtensionSchema.Tables.PNI.tableName());

    messagesManager = mock(MessagesManager.class);
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    final ProfilesManager profilesManager = mock(ProfilesManager.class);
    when(profilesManager.deleteAll(any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager =
        new PhoneNumberRecoveryPasswordsManager(new PhoneNumberRecoveryPasswords(
            DynamoDbExtensionSchema.Tables.PHONE_NUMBER_RECOVERY_PASSWORDS.tableName(),
            Duration.ofDays(1),
            DYNAMO_DB_EXTENSION.getDynamoDbClient(),
            Clock.systemUTC()));

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
        mock(ChangeNumberWaitingPeriodManager.class),
        secureStorageClient,
        svr2Client,
        mock(DisconnectionRequestManager.class),
        phoneNumberRecoveryPasswordsManager,
        scheduledExecutorService,
        scheduledExecutorService,
        clock,
        "link-device-secret".getBytes(StandardCharsets.UTF_8),
        AccountsManager.TOTP.getTimeStep().dividedBy(2));

    accountsManager.start();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    accountsManager.stop();
  }

  @Test
  void addDevice() throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final Account account = AccountsHelper.createAccount(accountsManager, number);
    assertEquals(1, accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getDevices().size());

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    Set.of(),
                    new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
                    Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
                    true,
                    Optional.empty(),
                    Optional.empty()),
                accountsManager.generateLinkDeviceToken(account.getAccountIdentifier()));

    assertEquals(2, updatedAccountAndDevice.first().getDevices().size());

    assertEquals(2,
        accountsManager.getByAccountIdentifier(updatedAccountAndDevice.first().getAccountIdentifier()).orElseThrow().getDevices()
            .size());

    final byte addedDeviceId = updatedAccountAndDevice.second().getId();

    assertTrue(
        keysManager.getEcSignedPreKey(updatedAccountAndDevice.first().getAccountIdentifier(), addedDeviceId).join().isPresent());
    assertTrue(
        keysManager.getEcSignedPreKey(updatedAccountAndDevice.first().getPhoneNumberIdentifier().orElseThrow(), addedDeviceId).join()
            .isPresent());
    assertTrue(keysManager.getLastResort(updatedAccountAndDevice.first().getAccountIdentifier(), addedDeviceId).join().isPresent());
    assertTrue(
        keysManager.getLastResort(updatedAccountAndDevice.first().getPhoneNumberIdentifier().orElseThrow(), addedDeviceId).join()
            .isPresent());
  }

  @Test
  void addDeviceReusedToken() throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final Account account = AccountsHelper.createAccount(accountsManager, number);
    assertEquals(1, accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getDevices().size());

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(account.getAccountIdentifier());

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    Set.of(),
                    new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
                    Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
                    true,
                    Optional.empty(),
                    Optional.empty()),
                linkDeviceToken);

    assertEquals(2,
        accountsManager.getByAccountIdentifier(updatedAccountAndDevice.first().getAccountIdentifier()).orElseThrow().getDevices()
            .size());

    assertThrows(LinkDeviceTokenAlreadyUsedException.class,
        () -> accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    Set.of(),
                    new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
                    Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
                    true,
                    Optional.empty(),
                    Optional.empty()),
                linkDeviceToken));

    assertEquals(2,
        accountsManager.getByAccountIdentifier(updatedAccountAndDevice.first().getAccountIdentifier()).orElseThrow().getDevices()
            .size());
  }

  @Test
  void removeDevice() throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final Account account = AccountsHelper.createAccount(accountsManager, number);
    assertEquals(1, accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getDevices().size());

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    Set.of(),
                    new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
                    Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
                    true,
                    Optional.empty(),
                    Optional.empty()),
                accountsManager.generateLinkDeviceToken(account.getAccountIdentifier()));

    final byte addedDeviceId = updatedAccountAndDevice.second().getId();

    final Account updatedAccount = accountsManager.removeDevice(updatedAccountAndDevice.first().getAccountIdentifier(), addedDeviceId);

    assertEquals(1, updatedAccount.getDevices().size());

    assertFalse(keysManager.getEcSignedPreKey(updatedAccount.getAccountIdentifier(), addedDeviceId).join().isPresent());
    assertFalse(
        keysManager.getEcSignedPreKey(updatedAccount.getPhoneNumberIdentifier().orElseThrow(), addedDeviceId).join().isPresent());
    assertFalse(keysManager.getLastResort(updatedAccount.getAccountIdentifier(), addedDeviceId).join().isPresent());
    assertFalse(keysManager.getLastResort(updatedAccount.getPhoneNumberIdentifier().orElseThrow(), addedDeviceId).join().isPresent());

    assertTrue(keysManager.getEcSignedPreKey(updatedAccount.getAccountIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(
        keysManager.getEcSignedPreKey(updatedAccount.getPhoneNumberIdentifier().orElseThrow(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getLastResort(updatedAccount.getAccountIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(
        keysManager.getLastResort(updatedAccount.getPhoneNumberIdentifier().orElseThrow(), Device.PRIMARY_ID).join().isPresent());
  }

  @Test
  void removeDevicePartialFailure() throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final Account account = AccountsHelper.createAccount(accountsManager, number);
    assertEquals(1, accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getDevices().size());

    final UUID aci = account.getAccountIdentifier();

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    Set.of(),
                    new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
                    Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
                    true,
                    Optional.empty(),
                    Optional.empty()),
                accountsManager.generateLinkDeviceToken(account.getAccountIdentifier()));

    final byte addedDeviceId = updatedAccountAndDevice.second().getId();

    when(messagesManager.clear(any(), anyByte()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("OH NO")));

    assertThrows(RuntimeException.class,
        () -> accountsManager.removeDevice(updatedAccountAndDevice.first().getAccountIdentifier(), addedDeviceId));

    final Account retrievedAccount = accountsManager.getByAccountIdentifierAsync(aci).join().orElseThrow();

    assertEquals(2, retrievedAccount.getDevices().size());

    assertTrue(keysManager.getEcSignedPreKey(retrievedAccount.getAccountIdentifier(), addedDeviceId).join().isPresent());
    assertTrue(
        keysManager.getEcSignedPreKey(retrievedAccount.getPhoneNumberIdentifier().orElseThrow(), addedDeviceId).join().isPresent());
    assertTrue(keysManager.getLastResort(retrievedAccount.getAccountIdentifier(), addedDeviceId).join().isPresent());
    assertTrue(
        keysManager.getLastResort(retrievedAccount.getPhoneNumberIdentifier().orElseThrow(), addedDeviceId).join().isPresent());

    assertTrue(keysManager.getEcSignedPreKey(retrievedAccount.getAccountIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(keysManager.getEcSignedPreKey(retrievedAccount.getPhoneNumberIdentifier().orElseThrow(), Device.PRIMARY_ID).join()
        .isPresent());
    assertTrue(keysManager.getLastResort(retrievedAccount.getAccountIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertTrue(
        keysManager.getLastResort(retrievedAccount.getPhoneNumberIdentifier().orElseThrow(), Device.PRIMARY_ID).join().isPresent());
  }

  @Test
  void waitForNewLinkedDevice() throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final Account account = AccountsHelper.createAccount(accountsManager, number);

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(account.getAccountIdentifier());
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    final CompletableFuture<Optional<DeviceInfo>> displacedFuture = accountsManager.waitForNewLinkedDevice(
        account.getAccountIdentifier(), account.getPrimaryDevice(),
        linkDeviceTokenIdentifier, Duration.ofSeconds(5));

    when(messagesManager.getEarliestUndeliveredTimestampForDevice(account.getAccountIdentifier(), account.getPrimaryDevice()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    final CompletableFuture<Optional<DeviceInfo>> activeFuture =
        accountsManager.waitForNewLinkedDevice(account.getAccountIdentifier(), account.getPrimaryDevice(), linkDeviceTokenIdentifier,
            Duration.ofSeconds(5));

    assertEquals(Optional.empty(), displacedFuture.join());

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    Set.of(),
                    new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
                    Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
                    true,
                    Optional.empty(),
                    Optional.empty()),
                linkDeviceToken);

    final Optional<DeviceInfo> maybeDeviceInfo = activeFuture.join();

    assertTrue(maybeDeviceInfo.isPresent());
    final DeviceInfo deviceInfo = maybeDeviceInfo.get();

    assertEquals(updatedAccountAndDevice.second().getId(), deviceInfo.id());
    assertEquals(updatedAccountAndDevice.second().getAccountRegistrationId(), deviceInfo.registrationId());
    assertNotNull(deviceInfo.createdAtCiphertext());
  }

  @Test
  void waitForNewLinkedDeviceAlreadyAdded() throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final Account account = AccountsHelper.createAccount(accountsManager, number);

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(account.getAccountIdentifier());
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    final Pair<Account, Device> updatedAccountAndDevice =
        accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
                    "device-name".getBytes(StandardCharsets.UTF_8),
                    "password",
                    "OWT",
                    Set.of(),
                    new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
                    Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
                    true,
                    Optional.empty(),
                    Optional.empty()),
                linkDeviceToken);

    when(messagesManager.getEarliestUndeliveredTimestampForDevice(account.getAccountIdentifier(), account.getPrimaryDevice()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final CompletableFuture<Optional<DeviceInfo>> linkedDeviceFuture = accountsManager.waitForNewLinkedDevice(
        account.getAccountIdentifier(), account.getPrimaryDevice(), linkDeviceTokenIdentifier, Duration.ofMinutes(1));

    final Optional<DeviceInfo> maybeDeviceInfo = linkedDeviceFuture.join();

    assertTrue(maybeDeviceInfo.isPresent());
    final DeviceInfo deviceInfo = maybeDeviceInfo.get();

    assertEquals(updatedAccountAndDevice.second().getId(), deviceInfo.id());
    assertEquals(updatedAccountAndDevice.second().getAccountRegistrationId(), deviceInfo.registrationId());
    assertNotNull(deviceInfo.createdAtCiphertext());
  }

  @Test
  void waitForNewLinkedDeviceTimeout() {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);
    final Account account = AccountsHelper.createAccount(accountsManager, number);

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(UUID.randomUUID());
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    final CompletableFuture<Optional<DeviceInfo>> linkedDeviceFuture = accountsManager.waitForNewLinkedDevice(
        account.getAccountIdentifier(), account.getPrimaryDevice(), linkDeviceTokenIdentifier, Duration.ofMillis(1));

    final Optional<DeviceInfo> maybeDeviceInfo = linkedDeviceFuture.join();

    assertTrue(maybeDeviceInfo.isEmpty());
  }

  @ParameterizedTest
  @CsvSource({
      "10_000,,false",         // no pending messages
      "10_000,9999,true",      // pending message right before now
      "10_000,10_000,false",   // pending message at now
      "10_000,10_001,false",   // pending message after now
  })
  void waitForMessageFetch(long currentTime, Long oldestMessage, boolean shouldWait)
      throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);
    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();
    final Account account = AccountsHelper.createAccount(accountsManager, number);

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(UUID.randomUUID());
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
            "device-name".getBytes(StandardCharsets.UTF_8),
            "password",
            "OWT",
            Set.of(),
            new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
            Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
            true,
            Optional.empty(),
            Optional.empty()),
        linkDeviceToken);

    when(messagesManager.getEarliestUndeliveredTimestampForDevice(account.getAccountIdentifier(), account.getPrimaryDevice()))
        .thenReturn(CompletableFuture.completedFuture(Optional.ofNullable(oldestMessage).map(Instant::ofEpochMilli)));

    clock.pin(Instant.ofEpochMilli(currentTime));
    Duration timeout = shouldWait ? Duration.ofMillis(5) : Duration.ofMillis(1000);
    Optional<DeviceInfo> result = accountsManager.waitForNewLinkedDevice(account.getAccountIdentifier(),
        account.getPrimaryDevice(), linkDeviceTokenIdentifier, timeout).join();
    assertEquals(result.isEmpty(), shouldWait);
  }

  // ThreadMode.SEPARATE_THREAD protects against hangs in the async calls, as this mode allows the test code to be
  // preempted by the timeout check
  @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @Test
  void waitForMessageFetchRetries() throws LinkDeviceTokenAlreadyUsedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);
    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();
    final Account account = AccountsHelper.createAccount(accountsManager, number);

    final String linkDeviceToken = accountsManager.generateLinkDeviceToken(UUID.randomUUID());
    final String linkDeviceTokenIdentifier = AccountsManager.getLinkDeviceTokenIdentifier(linkDeviceToken);

    clock.pin(Instant.ofEpochMilli(0));
    accountsManager.addDevice(account.getAccountIdentifier(), new DeviceSpec(
            "device-name".getBytes(StandardCharsets.UTF_8),
            "password",
            "OWT",
            Set.of(),
            new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
            Optional.of(new DeviceIdentityInfo(2, KeysHelper.signedECPreKey(2, pniKeyPair), KeysHelper.signedKEMPreKey(4, pniKeyPair))),
            true,
            Optional.empty(),
            Optional.empty()),
        linkDeviceToken);

    when(messagesManager.getEarliestUndeliveredTimestampForDevice(account.getAccountIdentifier(), account.getPrimaryDevice()))
        // Has a message older than the message epoch
        .thenReturn(CompletableFuture.completedFuture(Optional.of(Instant.ofEpochMilli(1000))))
        // The message was fetched
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    clock.pin(Instant.ofEpochMilli(10_000));
    // Run any scheduled job right away
    when(scheduledExecutorService.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(x -> {
      x.getArgument(0, Runnable.class).run();
      return null;
    });
    Optional<DeviceInfo> result = accountsManager.waitForNewLinkedDevice(account.getAccountIdentifier(),
        account.getPrimaryDevice(), linkDeviceTokenIdentifier, Duration.ofSeconds(10)).join();
    assertTrue(result.isPresent());
  }
}
