package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

public class AccountCreationDeletionIntegrationTest {

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

  record DeliveryChannels(boolean fetchesMessages, String apnsToken, String apnsVoipToken, String fcmToken) {}

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
    when(messagesManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));

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

  @CartesianTest
  @CartesianTest.MethodFactory("createAccount")
  void createAccount(final DeliveryChannels deliveryChannels,
      final boolean discoverableByPhoneNumber) throws InterruptedException {

    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final String password = RandomStringUtils.randomAlphanumeric(16);
    final String signalAgent = RandomStringUtils.randomAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final int pniRegistrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.randomAlphabetic(16).getBytes(StandardCharsets.UTF_8);
    final String registrationLockSecret = RandomStringUtils.randomAlphanumeric(16);

    final Device.DeviceCapabilities deviceCapabilities = new Device.DeviceCapabilities(
        ThreadLocalRandom.current().nextBoolean(),
        ThreadLocalRandom.current().nextBoolean(),
        ThreadLocalRandom.current().nextBoolean());

    final AccountAttributes accountAttributes = new AccountAttributes(deliveryChannels.fetchesMessages(),
        registrationId,
        pniRegistrationId,
        deviceName,
        registrationLockSecret,
        discoverableByPhoneNumber, deviceCapabilities);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.randomAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    final Optional<ApnRegistrationId> maybeApnRegistrationId =
        deliveryChannels.apnsToken() != null || deliveryChannels.apnsVoipToken() != null
            ? Optional.of(new ApnRegistrationId(deliveryChannels.apnsToken(), deliveryChannels.apnsVoipToken()))
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
            pniPqLastResortPreKey));

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
            new DeliveryChannels(true, null, null, null),
            new DeliveryChannels(false, "apns-token", null, null),
            new DeliveryChannels(false, "apns-token", "apns-voip-token", null),
            new DeliveryChannels(false, null, null, "fcm-token"))

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
          new AccountAttributes(true, 1, 1, "name".getBytes(StandardCharsets.UTF_8), "registration-lock", false, new Device.DeviceCapabilities(false, false, false)),
          Collections.emptyList(),
          new IdentityKey(aciKeyPair.getPublicKey()),
          new IdentityKey(pniKeyPair.getPublicKey()),
          new DeviceSpec(null,
              "password?",
              "OWI",
              new Device.DeviceCapabilities(false, false, false),
              1,
              2,
              true,
              Optional.empty(),
              Optional.empty(),
              aciSignedPreKey,
              pniSignedPreKey,
              aciPqLastResortPreKey,
              pniPqLastResortPreKey));

      existingAccountUuid = originalAccount.getUuid();
    }

    final String password = RandomStringUtils.randomAlphanumeric(16);
    final String signalAgent = RandomStringUtils.randomAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final int pniRegistrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.randomAlphabetic(16).getBytes(StandardCharsets.UTF_8);
    final String registrationLockSecret = RandomStringUtils.randomAlphanumeric(16);

    final Device.DeviceCapabilities deviceCapabilities = new Device.DeviceCapabilities(
        ThreadLocalRandom.current().nextBoolean(),
        ThreadLocalRandom.current().nextBoolean(),
        ThreadLocalRandom.current().nextBoolean());

    final AccountAttributes accountAttributes = new AccountAttributes(deliveryChannels.fetchesMessages(),
        registrationId,
        pniRegistrationId,
        deviceName,
        registrationLockSecret,
        discoverableByPhoneNumber,
        deviceCapabilities);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.randomAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    final Optional<ApnRegistrationId> maybeApnRegistrationId =
        deliveryChannels.apnsToken() != null || deliveryChannels.apnsVoipToken() != null
            ? Optional.of(new ApnRegistrationId(deliveryChannels.apnsToken(), deliveryChannels.apnsVoipToken()))
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
            pniPqLastResortPreKey));

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
  }

  @Test
  void deleteAccount() throws InterruptedException {
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final String password = RandomStringUtils.randomAlphanumeric(16);
    final String signalAgent = RandomStringUtils.randomAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final int pniRegistrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.randomAlphabetic(16).getBytes(StandardCharsets.UTF_8);
    final String registrationLockSecret = RandomStringUtils.randomAlphanumeric(16);

    final Device.DeviceCapabilities deviceCapabilities = new Device.DeviceCapabilities(
        ThreadLocalRandom.current().nextBoolean(),
        ThreadLocalRandom.current().nextBoolean(),
        ThreadLocalRandom.current().nextBoolean());

    final AccountAttributes accountAttributes = new AccountAttributes(true,
        registrationId,
        pniRegistrationId,
        deviceName,
        registrationLockSecret,
        true,
        deviceCapabilities);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.randomAlphabetic(8),
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
            pniPqLastResortPreKey));

    final UUID aci = account.getIdentifier(IdentityType.ACI);

    assertTrue(accountsManager.getByAccountIdentifier(aci).isPresent());

    accountsManager.delete(account, AccountsManager.DeletionReason.ADMIN_DELETED).join();

    assertFalse(accountsManager.getByAccountIdentifier(aci).isPresent());
    assertFalse(keysManager.getEcSignedPreKey(account.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(keysManager.getEcSignedPreKey(account.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(keysManager.getLastResort(account.getUuid(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(keysManager.getLastResort(account.getPhoneNumberIdentifier(), Device.PRIMARY_ID).join().isPresent());
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
      final Device.DeviceCapabilities deviceCapabilities,
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
    assertEquals(registrationId, primaryDevice.getRegistrationId());
    assertEquals(pniRegistrationId, primaryDevice.getPhoneNumberIdentityRegistrationId().orElseThrow());
    assertArrayEquals(deviceName, primaryDevice.getName());
    assertEquals(discoverableByPhoneNumber, account.isDiscoverableByPhoneNumber());
    assertEquals(deviceCapabilities, primaryDevice.getCapabilities());
    assertEquals(badges, account.getBadges());

    maybeApnRegistrationId.ifPresentOrElse(apnRegistrationId -> {
      assertEquals(apnRegistrationId.apnRegistrationId(), primaryDevice.getApnId());
      assertEquals(apnRegistrationId.voipRegistrationId(), primaryDevice.getVoipApnId());
    }, () -> {
      assertNull(primaryDevice.getApnId());
      assertNull(primaryDevice.getVoipApnId());
    });

    maybeGcmRegistrationId.ifPresentOrElse(gcmRegistrationId -> {
      assertEquals(deliveryChannels.fcmToken(), primaryDevice.getGcmId());
    }, () -> {
      assertNull(primaryDevice.getGcmId());
    });

    assertTrue(account.getRegistrationLock().verify(registrationLockSecret));
    assertTrue(primaryDevice.getAuthTokenHash().verify(password));

    assertEquals(Optional.of(aciSignedPreKey), keysManager.getEcSignedPreKey(account.getIdentifier(IdentityType.ACI), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniSignedPreKey), keysManager.getEcSignedPreKey(account.getIdentifier(IdentityType.PNI), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(aciPqLastResortPreKey), keysManager.getLastResort(account.getIdentifier(IdentityType.ACI), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniPqLastResortPreKey), keysManager.getLastResort(account.getIdentifier(IdentityType.PNI), Device.PRIMARY_ID).join());
  }
}
