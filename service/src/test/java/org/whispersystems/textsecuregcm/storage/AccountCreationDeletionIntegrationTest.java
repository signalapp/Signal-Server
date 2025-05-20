package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class AccountCreationDeletionIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.ACCOUNTS,
      DynamoDbExtensionSchema.Tables.CLIENT_PUBLIC_KEYS,
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

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension("testbucket");

  private static final Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private ScheduledExecutorService executor;

  private AccountsManager accountsManager;
  private KeysManager keysManager;
  private ClientPublicKeysManager clientPublicKeysManager;
  private DisconnectionRequestManager disconnectionRequestManager;

  record DeliveryChannels(boolean fetchesMessages, String apnsToken, String fcmToken) {}

  @BeforeEach
  void setUp() {
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    final DynamoDbAsyncClient dynamoDbAsyncClient = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient();
    keysManager = new KeysManager(
        new SingleUseECPreKeyStore(dynamoDbAsyncClient, DynamoDbExtensionSchema.Tables.EC_KEYS.tableName()),
        new SingleUseKEMPreKeyStore(dynamoDbAsyncClient, DynamoDbExtensionSchema.Tables.PQ_KEYS.tableName()),
        new PagedSingleUseKEMPreKeyStore(dynamoDbAsyncClient,
            S3_EXTENSION.getS3Client(),
            DynamoDbExtensionSchema.Tables.PAGED_PQ_KEYS.tableName(),
            S3_EXTENSION.getBucketName()),
        new RepeatedUseECSignedPreKeyStore(dynamoDbAsyncClient,
            DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName()),
        new RepeatedUseKEMSignedPreKeyStore(dynamoDbAsyncClient,
            DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName()));

    final ClientPublicKeys clientPublicKeys = new ClientPublicKeys(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.CLIENT_PUBLIC_KEYS.tableName());

    final Accounts accounts = new Accounts(
        CLOCK,
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.NUMBERS.tableName(),
        DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS.tableName(),
        DynamoDbExtensionSchema.Tables.USERNAMES.tableName(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.USED_LINK_DEVICE_TOKENS.tableName());

    executor = Executors.newSingleThreadScheduledExecutor();

    final AccountLockManager accountLockManager = new AccountLockManager(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK.tableName());

    clientPublicKeysManager = new ClientPublicKeysManager(clientPublicKeys, accountLockManager, executor);

    final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
    when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureValueRecovery2Client svr2Client = mock(SecureValueRecovery2Client.class);
    when(svr2Client.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
            DynamoDbExtensionSchema.Tables.PNI.tableName());

    final MessagesManager messagesManager = mock(MessagesManager.class);
    when(messagesManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));

    final ProfilesManager profilesManager = mock(ProfilesManager.class);
    when(profilesManager.deleteAll(any())).thenReturn(CompletableFuture.completedFuture(null));

    final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        mock(RegistrationRecoveryPasswordsManager.class);

    when(registrationRecoveryPasswordsManager.remove(any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    disconnectionRequestManager = mock(DisconnectionRequestManager.class);
    when(disconnectionRequestManager.requestDisconnection(any())).thenReturn(CompletableFuture.completedFuture(null));

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        CACHE_CLUSTER_EXTENSION.getRedisCluster(),
        mock(FaultTolerantRedisClient.class),
        accountLockManager,
        keysManager,
        messagesManager,
        profilesManager,
        secureStorageClient,
        svr2Client,
        disconnectionRequestManager,
        registrationRecoveryPasswordsManager,
        clientPublicKeysManager,
        executor,
        executor,
        CLOCK,
        "link-device-secret".getBytes(StandardCharsets.UTF_8),
        dynamicConfigurationManager);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdown();

    //noinspection ResultOfMethodCallIgnored
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @CartesianTest
  @CartesianTest.MethodFactory("createAccount")
  void createAccount(final DeliveryChannels deliveryChannels,
      final boolean discoverableByPhoneNumber) throws InterruptedException {

    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final String password = RandomStringUtils.secure().nextAlphanumeric(16);
    final String signalAgent = RandomStringUtils.secure().nextAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final int pniRegistrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.secure().nextAlphabetic(16).getBytes(StandardCharsets.UTF_8);
    final String registrationLockSecret = RandomStringUtils.secure().nextAlphanumeric(16);

    final Set<DeviceCapability> deviceCapabilities = Set.of();

    final AccountAttributes accountAttributes = new AccountAttributes(deliveryChannels.fetchesMessages(),
        registrationId,
        pniRegistrationId,
        deviceName,
        registrationLockSecret,
        discoverableByPhoneNumber,
        deviceCapabilities);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    final Optional<ApnRegistrationId> maybeApnRegistrationId =
        deliveryChannels.apnsToken() != null
            ? Optional.of(new ApnRegistrationId(deliveryChannels.apnsToken()))
            : Optional.empty();

    final Optional<GcmRegistrationId> maybeGcmRegistrationId = deliveryChannels.fcmToken() != null
        ? Optional.of(new GcmRegistrationId(deliveryChannels.fcmToken()))
        : Optional.empty();

    final Account account = accountsManager.create(number,
        accountAttributes,
        badges,
        new IdentityKey(aciKeyPair.getPublicKey()),
        new IdentityKey(pniKeyPair.getPublicKey()),
        new DeviceSpec(
            deviceName,
            password,
            signalAgent,
            deviceCapabilities,
            registrationId,
            pniRegistrationId,
            deliveryChannels.fetchesMessages(),
            maybeApnRegistrationId,
            maybeGcmRegistrationId,
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey),
        null);

    assertExpectedStoredAccount(account,
        number,
        password,
        signalAgent,
        deliveryChannels,
        registrationId,
        pniRegistrationId,
        deviceName,
        discoverableByPhoneNumber,
        deviceCapabilities,
        badges,
        maybeApnRegistrationId,
        maybeGcmRegistrationId,
        registrationLockSecret,
        aciSignedPreKey,
        pniSignedPreKey,
        aciPqLastResortPreKey,
        pniPqLastResortPreKey);

    assertEquals(Optional.of(aciSignedPreKey), keysManager.getEcSignedPreKey(account.getUuid(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniSignedPreKey), keysManager.getEcSignedPreKey(account.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(aciPqLastResortPreKey), keysManager.getLastResort(account.getUuid(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniPqLastResortPreKey), keysManager.getLastResort(account.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join());
  }

  @SuppressWarnings("unused")
  static ArgumentSets createAccount() {
    return ArgumentSets
        // deliveryChannels
        .argumentsForFirstParameter(
            new DeliveryChannels(true, null, null),
            new DeliveryChannels(false, "apns-token", null),
            new DeliveryChannels(false, "apns-token", null),
            new DeliveryChannels(false, null, "fcm-token"))

        // discoverableByPhoneNumber
        .argumentsForNextParameter(true, false);
  }

  @CartesianTest
  @CartesianTest.MethodFactory("createAccount")
  void reregisterAccount(final DeliveryChannels deliveryChannels,
      final boolean discoverableByPhoneNumber) throws InterruptedException {

    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final UUID existingAccountUuid;
    {
      final ECKeyPair aciKeyPair = Curve.generateKeyPair();
      final ECKeyPair pniKeyPair = Curve.generateKeyPair();

      final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
      final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
      final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
      final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

      final Account originalAccount = accountsManager.create(number,
          new AccountAttributes(true, 1, 1, "name".getBytes(StandardCharsets.UTF_8), "registration-lock", false, Set.of()),
          Collections.emptyList(),
          new IdentityKey(aciKeyPair.getPublicKey()),
          new IdentityKey(pniKeyPair.getPublicKey()),
          new DeviceSpec(null,
              "password?",
              "OWI",
              Set.of(),
              1,
              2,
              true,
              Optional.empty(),
              Optional.empty(),
              aciSignedPreKey,
              pniSignedPreKey,
              aciPqLastResortPreKey,
              pniPqLastResortPreKey),
          null);

      existingAccountUuid = originalAccount.getUuid();
    }

    final String password = RandomStringUtils.secure().nextAlphanumeric(16);
    final String signalAgent = RandomStringUtils.secure().nextAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final int pniRegistrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.secure().nextAlphabetic(16).getBytes(StandardCharsets.UTF_8);
    final String registrationLockSecret = RandomStringUtils.secure().nextAlphanumeric(16);

    final Set<DeviceCapability> deviceCapabilities = Set.of();

    final AccountAttributes accountAttributes = new AccountAttributes(deliveryChannels.fetchesMessages(),
        registrationId,
        pniRegistrationId,
        deviceName,
        registrationLockSecret,
        discoverableByPhoneNumber,
        deviceCapabilities);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    final Optional<ApnRegistrationId> maybeApnRegistrationId =
        deliveryChannels.apnsToken() != null
            ? Optional.of(new ApnRegistrationId(deliveryChannels.apnsToken()))
            : Optional.empty();

    final Optional<GcmRegistrationId> maybeGcmRegistrationId = deliveryChannels.fcmToken() != null
        ? Optional.of(new GcmRegistrationId(deliveryChannels.fcmToken()))
        : Optional.empty();

    final Account reregisteredAccount = accountsManager.create(number,
        accountAttributes,
        badges,
        new IdentityKey(aciKeyPair.getPublicKey()),
        new IdentityKey(pniKeyPair.getPublicKey()),
        new DeviceSpec(deviceName,
            password,
            signalAgent,
            deviceCapabilities,
            registrationId,
            pniRegistrationId,
            accountAttributes.getFetchesMessages(),
            maybeApnRegistrationId,
            maybeGcmRegistrationId,
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey),
        null);

    assertExpectedStoredAccount(reregisteredAccount,
        number,
        password,
        signalAgent,
        deliveryChannels,
        registrationId,
        pniRegistrationId,
        deviceName,
        discoverableByPhoneNumber,
        deviceCapabilities,
        badges,
        maybeApnRegistrationId,
        maybeGcmRegistrationId,
        registrationLockSecret,
        aciSignedPreKey,
        pniSignedPreKey,
        aciPqLastResortPreKey,
        pniPqLastResortPreKey);

    assertEquals(existingAccountUuid, reregisteredAccount.getUuid());

    verify(disconnectionRequestManager).requestDisconnection(existingAccountUuid);
  }

  @Test
  void deleteAccount() throws InterruptedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final String password = RandomStringUtils.secure().nextAlphanumeric(16);
    final String signalAgent = RandomStringUtils.secure().nextAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final int pniRegistrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.secure().nextAlphabetic(16).getBytes(StandardCharsets.UTF_8);
    final String registrationLockSecret = RandomStringUtils.secure().nextAlphanumeric(16);

    final Set<DeviceCapability> deviceCapabilities = Set.of();

    final AccountAttributes accountAttributes = new AccountAttributes(true,
        registrationId,
        pniRegistrationId,
        deviceName,
        registrationLockSecret,
        true,
        deviceCapabilities);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    final Account account = accountsManager.create(number,
        accountAttributes,
        badges,
        new IdentityKey(aciKeyPair.getPublicKey()),
        new IdentityKey(pniKeyPair.getPublicKey()),
        new DeviceSpec(
            deviceName,
            password,
            signalAgent,
            deviceCapabilities,
            registrationId,
            pniRegistrationId,
            true,
            Optional.empty(),
            Optional.empty(),
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey),
        null);

    clientPublicKeysManager.setPublicKey(account, Device.PRIMARY_ID, Curve.generateKeyPair().getPublicKey()).join();

    final UUID aci = account.getIdentifier(IdentityType.ACI);

    assertTrue(accountsManager.getByAccountIdentifier(aci).isPresent());

    accountsManager.delete(account, AccountsManager.DeletionReason.ADMIN_DELETED).join();

    assertFalse(accountsManager.getByAccountIdentifier(aci).isPresent());
    assertFalse(keysManager.getEcSignedPreKey(account.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(keysManager.getEcSignedPreKey(account.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(keysManager.getLastResort(account.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(keysManager.getLastResort(account.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(clientPublicKeysManager.findPublicKey(account.getUuid(), Device.PRIMARY_ID).join().isPresent());

    verify(disconnectionRequestManager).requestDisconnection(aci);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void assertExpectedStoredAccount(final Account account,
      final String number,
      final String password,
      final String signalAgent,
      final DeliveryChannels deliveryChannels,
      final int registrationId,
      final int pniRegistrationId,
      final byte[] deviceName,
      final boolean discoverableByPhoneNumber,
      final Set<DeviceCapability> deviceCapabilities,
      final List<AccountBadge> badges,
      final Optional<ApnRegistrationId> maybeApnRegistrationId,
      final Optional<GcmRegistrationId> maybeGcmRegistrationId,
      final String registrationLockSecret,
      final ECSignedPreKey aciSignedPreKey,
      final ECSignedPreKey pniSignedPreKey,
      final KEMSignedPreKey aciPqLastResortPreKey,
      final KEMSignedPreKey pniPqLastResortPreKey) {

    final Device primaryDevice = account.getPrimaryDevice();

    assertEquals(number, account.getNumber());
    assertEquals(signalAgent, primaryDevice.getUserAgent());
    assertEquals(deliveryChannels.fetchesMessages(), primaryDevice.getFetchesMessages());
    assertEquals(registrationId, primaryDevice.getRegistrationId(IdentityType.ACI));
    assertEquals(pniRegistrationId, primaryDevice.getRegistrationId(IdentityType.PNI));
    assertArrayEquals(deviceName, primaryDevice.getName());
    assertEquals(discoverableByPhoneNumber, account.isDiscoverableByPhoneNumber());
    assertEquals(deviceCapabilities, primaryDevice.getCapabilities());
    assertEquals(badges, account.getBadges());

    maybeApnRegistrationId.ifPresentOrElse(
        apnRegistrationId -> assertEquals(apnRegistrationId.apnRegistrationId(), primaryDevice.getApnId()),
        () -> assertNull(primaryDevice.getApnId()));

    maybeGcmRegistrationId.ifPresentOrElse(
        gcmRegistrationId -> assertEquals(deliveryChannels.fcmToken(), primaryDevice.getGcmId()),
        () -> assertNull(primaryDevice.getGcmId()));

    assertTrue(account.getRegistrationLock().verify(registrationLockSecret));
    assertTrue(primaryDevice.getAuthTokenHash().verify(password));

    assertEquals(Optional.of(aciSignedPreKey), keysManager.getEcSignedPreKey(account.getIdentifier(IdentityType.ACI), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniSignedPreKey), keysManager.getEcSignedPreKey(account.getIdentifier(IdentityType.PNI), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(aciPqLastResortPreKey), keysManager.getLastResort(account.getIdentifier(IdentityType.ACI), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniPqLastResortPreKey), keysManager.getLastResort(account.getIdentifier(IdentityType.PNI), Device.PRIMARY_ID).join());
  }
}
