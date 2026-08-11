package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.storage.ReceiptCredentialTestUtil.receiptPresentation;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

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
      DynamoDbExtensionSchema.Tables.PAGED_PQ_KEYS,
      DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS,
      DynamoDbExtensionSchema.Tables.PHONE_NUMBER_RECOVERY_PASSWORDS,
      DynamoDbExtensionSchema.Tables.REDEEMED_RECEIPTS);

  @RegisterExtension
  static final RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension("testbucket");

  private static final Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private ScheduledExecutorService executor;

  private AccountsManager accountsManager;
  private KeysManager keysManager;
  private PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager;
  private DisconnectionRequestManager disconnectionRequestManager;

  record DeliveryChannels(boolean fetchesMessages, String apnsToken, String fcmToken) {}

  @BeforeEach
  void setUp() {
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
        CLOCK,
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        new RedeemedReceiptsManager(CLOCK, DynamoDbExtensionSchema.Tables.REDEEMED_RECEIPTS.tableName(),
            DYNAMO_DB_EXTENSION.getDynamoDbClient(), Duration.ofDays(30)),
        DynamoDbExtensionSchema.Tables.ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.NUMBERS.tableName(),
        DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS.tableName(),
        DynamoDbExtensionSchema.Tables.USERNAMES.tableName(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.USED_LINK_DEVICE_TOKENS.tableName());

    executor = Executors.newSingleThreadScheduledExecutor();

    final AccountLockManager accountLockManager = new AccountLockManager(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK.tableName());

    final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
    when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureValueRecoveryClient svr2Client = mock(SecureValueRecoveryClient.class);
    when(svr2Client.removeData(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
            DynamoDbExtensionSchema.Tables.PNI.tableName());

    final MessagesManager messagesManager = mock(MessagesManager.class);
    when(messagesManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));

    final ProfilesManager profilesManager = mock(ProfilesManager.class);
    when(profilesManager.deleteAll(any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberRecoveryPasswords phoneNumberRecoveryPasswords =
        new PhoneNumberRecoveryPasswords(DynamoDbExtensionSchema.Tables.PHONE_NUMBER_RECOVERY_PASSWORDS.tableName(),
            Duration.ofDays(1),
            DYNAMO_DB_EXTENSION.getDynamoDbClient(),
            CLOCK);

    phoneNumberRecoveryPasswordsManager = new PhoneNumberRecoveryPasswordsManager(phoneNumberRecoveryPasswords);

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
        mock(ChangeNumberWaitingPeriodManager.class),
        secureStorageClient,
        svr2Client,
        disconnectionRequestManager,
        phoneNumberRecoveryPasswordsManager,
        executor,
        executor,
        executor,
        CLOCK,
        "link-device-secret".getBytes(StandardCharsets.UTF_8));
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
      final boolean discoverableByPhoneNumber) {

    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final String password = RandomStringUtils.secure().nextAlphanumeric(16);
    final String signalAgent = RandomStringUtils.secure().nextAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final int pniRegistrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.secure().nextAlphabetic(16).getBytes(StandardCharsets.UTF_8);
    final String registrationLockSecret = RandomStringUtils.secure().nextAlphanumeric(16);
    final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

    final Set<DeviceCapability> deviceCapabilities = Set.of();

    final AccountAttributes accountAttributes = new AccountAttributes(deliveryChannels.fetchesMessages(),
        registrationId,
        pniRegistrationId,
        deviceName,
        registrationLockSecret,
        discoverableByPhoneNumber,
        deviceCapabilities,
        recoveryPassword);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

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
            new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
            Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)),
            deliveryChannels.fetchesMessages(),
            maybeApnRegistrationId,
            maybeGcmRegistrationId),
        null);

    assertExpectedStoredAccount(account,
        Optional.of(number),
        password,
        signalAgent,
        deliveryChannels,
        registrationId,
        Optional.of(pniRegistrationId),
        deviceName,
        discoverableByPhoneNumber,
        deviceCapabilities,
        badges,
        maybeApnRegistrationId,
        maybeGcmRegistrationId,
        Optional.of(registrationLockSecret),
        aciSignedPreKey,
        Optional.of(pniSignedPreKey),
        aciPqLastResortPreKey,
        Optional.of(pniPqLastResortPreKey));

    assertEquals(Optional.of(aciSignedPreKey), keysManager.getEcSignedPreKey(account.getAccountIdentifier(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniSignedPreKey), keysManager.getEcSignedPreKey(account.getPhoneNumberIdentifierOptional().orElseThrow(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(aciPqLastResortPreKey), keysManager.getLastResort(account.getAccountIdentifier(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(pniPqLastResortPreKey), keysManager.getLastResort(account.getPhoneNumberIdentifierOptional().orElseThrow(), Device.PRIMARY_ID).join());
    assertTrue(phoneNumberRecoveryPasswordsManager.verify(account.getPhoneNumberIdentifierOptional().orElseThrow(), recoveryPassword));
  }

  @ParameterizedTest
  @MethodSource("deliveryChannels")
  void createAccountWithoutNumber(final DeliveryChannels deliveryChannels)
      throws InvalidInputException, VerificationFailedException, ReceiptAlreadyRedeemedException {
    final String password = RandomStringUtils.secure().nextAlphanumeric(16);
    final String signalAgent = RandomStringUtils.secure().nextAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.secure().nextAlphabetic(16).getBytes(StandardCharsets.UTF_8);

    final Set<DeviceCapability> deviceCapabilities = Set.of();

    final AccountAttributes accountAttributes = new AccountAttributes(deliveryChannels.fetchesMessages(),
        registrationId,
        null,
        deviceName,
        null,
        false,
        deviceCapabilities,
        TestRandomUtil.nextBytes(16));

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = ECKeyPair.generate();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);

    final Optional<ApnRegistrationId> maybeApnRegistrationId =
        deliveryChannels.apnsToken() != null
            ? Optional.of(new ApnRegistrationId(deliveryChannels.apnsToken()))
            : Optional.empty();

    final Optional<GcmRegistrationId> maybeGcmRegistrationId = deliveryChannels.fcmToken() != null
        ? Optional.of(new GcmRegistrationId(deliveryChannels.fcmToken()))
        : Optional.empty();

    final Account account = accountsManager.create(accountAttributes,
        badges,
        new IdentityKey(aciKeyPair.getPublicKey()),
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), 1),
        new DeviceSpec(
            deviceName,
            password,
            signalAgent,
            deviceCapabilities,
            new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
            Optional.empty(),
            deliveryChannels.fetchesMessages(),
            maybeApnRegistrationId,
            maybeGcmRegistrationId),
        null);

    assertExpectedStoredAccount(account,
        Optional.empty(),
        password,
        signalAgent,
        deliveryChannels,
        registrationId,
        Optional.empty(),
        deviceName,
        false,
        deviceCapabilities,
        badges,
        maybeApnRegistrationId,
        maybeGcmRegistrationId,
        Optional.empty(),
        aciSignedPreKey,
        Optional.empty(),
        aciPqLastResortPreKey,
        Optional.empty());

    assertEquals(Optional.of(aciSignedPreKey), keysManager.getEcSignedPreKey(account.getAccountIdentifier(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(aciPqLastResortPreKey), keysManager.getLastResort(account.getAccountIdentifier(), Device.PRIMARY_ID).join());

    assertTrue(account.getNumberOptional().isEmpty());
    assertTrue(account.getPhoneNumberIdentifierOptional().isEmpty());
    assertTrue(account.getPhoneNumberIdentityKey().isEmpty());
  }

  private static List<DeliveryChannels> deliveryChannels() {
    return List.of(
        new DeliveryChannels(true, null, null),
        new DeliveryChannels(false, "apns-token", null),
        new DeliveryChannels(false, "apns-token", null),
        new DeliveryChannels(false, null, "fcm-token"));
  }

  @SuppressWarnings("unused")
  static ArgumentSets createAccount() {
    return ArgumentSets.argumentsForFirstParameter(deliveryChannels())
        // discoverableByPhoneNumber
        .argumentsForNextParameter(true, false);
  }

  @CartesianTest
  @CartesianTest.MethodFactory("createAccount")
  void reregisterAccount(final DeliveryChannels deliveryChannels,
      final boolean discoverableByPhoneNumber) {

    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    final byte[] originalRecoveryPassword = TestRandomUtil.nextBytes(16);
    final byte[] updatedRecoveryPassword = TestRandomUtil.nextBytes(17);

    final UUID existingAccountUuid;
    {
      final ECKeyPair aciKeyPair = ECKeyPair.generate();
      final ECKeyPair pniKeyPair = ECKeyPair.generate();

      final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
      final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
      final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
      final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

      final Account existingAccount = accountsManager.create(number,
          new AccountAttributes(true, 1, 1, "name".getBytes(StandardCharsets.UTF_8), "registration-lock", false, Set.of(),
              originalRecoveryPassword),
          Collections.emptyList(),
          new IdentityKey(aciKeyPair.getPublicKey()),
          new IdentityKey(pniKeyPair.getPublicKey()),
          new DeviceSpec(null,
              "password?",
              "OWI",
              Set.of(),
              new DeviceIdentityInfo(1, aciSignedPreKey, aciPqLastResortPreKey),
              Optional.of(new DeviceIdentityInfo(2, pniSignedPreKey, pniPqLastResortPreKey)),
              true,
              Optional.empty(),
              Optional.empty()),
          null);

      existingAccountUuid = existingAccount.getAccountIdentifier();
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
        deviceCapabilities,
        updatedRecoveryPassword);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

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
            new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
            Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)),
            accountAttributes.getFetchesMessages(),
            maybeApnRegistrationId,
            maybeGcmRegistrationId),
        null);

    assertExpectedStoredAccount(reregisteredAccount,
        Optional.of(number),
        password,
        signalAgent,
        deliveryChannels,
        registrationId,
        Optional.of(pniRegistrationId),
        deviceName,
        discoverableByPhoneNumber,
        deviceCapabilities,
        badges,
        maybeApnRegistrationId,
        maybeGcmRegistrationId,
        Optional.of(registrationLockSecret),
        aciSignedPreKey,
        Optional.of(pniSignedPreKey),
        aciPqLastResortPreKey,
        Optional.of(pniPqLastResortPreKey));

    assertEquals(existingAccountUuid, reregisteredAccount.getAccountIdentifier());

    verify(disconnectionRequestManager).requestDisconnection(argThat(account ->
        account.getAccountIdentifier().equals(existingAccountUuid) && account != reregisteredAccount));

    assertTrue(phoneNumberRecoveryPasswordsManager.verify(reregisteredAccount.getPhoneNumberIdentifierOptional().orElseThrow(), updatedRecoveryPassword));
    assertFalse(phoneNumberRecoveryPasswordsManager.verify(reregisteredAccount.getPhoneNumberIdentifierOptional().orElseThrow(), originalRecoveryPassword));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deleteAccount(final boolean hasE164)
      throws InvalidInputException, VerificationFailedException, ReceiptAlreadyRedeemedException {
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
        deviceCapabilities, null);

    accountAttributes.setRecoveryPassword(TestRandomUtil.nextBytes(16));

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    final Account account;
    if (hasE164) {
      account = accountsManager.create(number,
          accountAttributes,
          badges,
          new IdentityKey(aciKeyPair.getPublicKey()),
          new IdentityKey(pniKeyPair.getPublicKey()),
          new DeviceSpec(
              deviceName,
              password,
              signalAgent,
              deviceCapabilities,
              new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
              Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)),
              true,
              Optional.empty(),
              Optional.empty()),
          null);
      assertTrue(phoneNumberRecoveryPasswordsManager.verify(account.getPhoneNumberIdentifierOptional().orElseThrow(),
          accountAttributes.recoveryPassword().orElseThrow()));
    } else {
      account = accountsManager.create(accountAttributes,
          badges,
          new IdentityKey(aciKeyPair.getPublicKey()),
          receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), 1),
          new DeviceSpec(
              deviceName,
              password,
              signalAgent,
              deviceCapabilities,
              new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
              Optional.empty(),
              true,
              Optional.empty(),
              Optional.empty()),
          null);
    }

    final UUID aci = account.getAccountIdentifier();

    assertTrue(accountsManager.getByAccountIdentifier(aci).isPresent());
    accountsManager.delete(account.getAccountIdentifier(), AccountsManager.DeletionReason.ADMIN_DELETED);

    assertFalse(accountsManager.getByAccountIdentifier(aci).isPresent());
    assertFalse(keysManager.getEcSignedPreKey(account.getAccountIdentifier(), Device.PRIMARY_ID).join().isPresent());
    assertFalse(keysManager.getLastResort(account.getAccountIdentifier(), Device.PRIMARY_ID).join().isPresent());
    if (hasE164) {
      assertFalse(keysManager.getEcSignedPreKey(account.getPhoneNumberIdentifierOptional().orElseThrow(), Device.PRIMARY_ID).join().isPresent());
      assertFalse(keysManager.getLastResort(account.getPhoneNumberIdentifierOptional().orElseThrow(), Device.PRIMARY_ID).join().isPresent());
      assertFalse(phoneNumberRecoveryPasswordsManager.verify(account.getPhoneNumberIdentifierOptional().orElseThrow(),
          accountAttributes.recoveryPassword().orElseThrow()));
    }

    verify(disconnectionRequestManager).requestDisconnection(argThat(disconnectedAccount ->
        disconnectedAccount.getAccountIdentifier().equals(account.getAccountIdentifier())));
  }

  @Test
  void retryRegistrationAccountWithoutNumber()
      throws InvalidInputException, VerificationFailedException, ReceiptAlreadyRedeemedException {

    final ReceiptSerial receiptSerial = new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE));
    final String password = RandomStringUtils.secure().nextAlphanumeric(16);
    final byte[] recoveryPassword = TestRandomUtil.nextBytes(32);
    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciKeyPair.getPublicKey());
    final DeliveryChannels deliveryChannels = new DeliveryChannels(false, "apns-token", null);

    final UUID existingAccountUuid;
    {
      final Account existingAccount = accountsManager.create(
          new AccountAttributes(true, 1, null, "name".getBytes(StandardCharsets.UTF_8), null, false, Set.of(), null)
              .setRecoveryPassword(recoveryPassword),
          Collections.emptyList(),
          aciIdentityKey,
          receiptPresentation(receiptSerial, CLOCK.instant().plus(Duration.ofDays(30)), 1),
          new DeviceSpec(null,
              password,
              "OWI",
              Set.of(),
              new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
              Optional.empty(),
              true,
              Optional.empty(),
              Optional.empty()),
          null);

      existingAccountUuid = existingAccount.getAccountIdentifier();
    }

    final String signalAgent = RandomStringUtils.secure().nextAlphabetic(3);
    final int registrationId = ThreadLocalRandom.current().nextInt(Device.MAX_REGISTRATION_ID);
    final byte[] deviceName = RandomStringUtils.secure().nextAlphabetic(16).getBytes(StandardCharsets.UTF_8);

    final Set<DeviceCapability> deviceCapabilities = Set.of();

    final AccountAttributes accountAttributes = new AccountAttributes(deliveryChannels.fetchesMessages(),
        registrationId,
        null,
        deviceName,
        null,
        false,
        deviceCapabilities, null).setRecoveryPassword(recoveryPassword);

    final List<AccountBadge> badges = new ArrayList<>(List.of(new AccountBadge(
        RandomStringUtils.secure().nextAlphabetic(8),
        CLOCK.instant().plus(Duration.ofDays(7)),
        true)));

    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(5, aciKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(7, aciKeyPair);

    final Optional<ApnRegistrationId> maybeApnRegistrationId =
        deliveryChannels.apnsToken() != null
            ? Optional.of(new ApnRegistrationId(deliveryChannels.apnsToken()))
            : Optional.empty();

    final Optional<GcmRegistrationId> maybeGcmRegistrationId = deliveryChannels.fcmToken() != null
        ? Optional.of(new GcmRegistrationId(deliveryChannels.fcmToken()))
        : Optional.empty();

    final Account retriedAccount = accountsManager.create(accountAttributes,
        badges,
        aciIdentityKey,
        receiptPresentation(receiptSerial,  CLOCK.instant().plus(Duration.ofDays(30)), 1),
        new DeviceSpec(deviceName,
            password,
            signalAgent,
            deviceCapabilities,
            new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
            Optional.empty(),
            accountAttributes.getFetchesMessages(),
            maybeApnRegistrationId,
            maybeGcmRegistrationId),
        null);

    assertExpectedStoredAccount(retriedAccount,
        Optional.empty(),
        password,
        signalAgent,
        deliveryChannels,
        registrationId,
        Optional.empty(),
        deviceName,
        false,
        deviceCapabilities,
        badges,
        maybeApnRegistrationId,
        maybeGcmRegistrationId,
        Optional.empty(),
        aciSignedPreKey,
        Optional.empty(),
        aciPqLastResortPreKey,
        Optional.empty());

    assertEquals(existingAccountUuid, retriedAccount.getAccountIdentifier());

    assertTrue(retriedAccount.getNumberOptional().isEmpty());
    assertTrue(retriedAccount.getPhoneNumberIdentifierOptional().isEmpty());
    assertTrue(retriedAccount.getPhoneNumberIdentityKey().isEmpty());

    verify(disconnectionRequestManager).requestDisconnection(argThat(account ->
        account.getAccountIdentifier().equals(existingAccountUuid) && account != retriedAccount));
  }

  @Test
  void retryRegistrationAccountWithNoNumberDifferentRecoveryPassword()
      throws InvalidInputException, VerificationFailedException, ReceiptAlreadyRedeemedException {

    final ReceiptSerial receiptSerial = new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE));
    final String password = RandomStringUtils.secure().nextAlphanumeric(16);
    final byte[] recoveryPassword = TestRandomUtil.nextBytes(32);
    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciKeyPair.getPublicKey());

    final Account existingAccount = accountsManager.create(
        new AccountAttributes(true, 1, null, "name".getBytes(StandardCharsets.UTF_8), null, false, Set.of(), recoveryPassword),
        Collections.emptyList(),
        aciIdentityKey,
        receiptPresentation(receiptSerial, CLOCK.instant().plus(Duration.ofDays(30)), 1),
        new DeviceSpec(null,
            password,
            "OWI",
            Set.of(),
            new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
            Optional.empty(),
            true,
            Optional.empty(),
            Optional.empty()),
        null);

    assertNotNull(existingAccount);

    assertThrows(ReceiptAlreadyRedeemedException.class, () -> accountsManager.create(
        // Using a different account recovery password should throw an exception
        new AccountAttributes(true, 1, null, "name".getBytes(StandardCharsets.UTF_8), null, false, Set.of(), TestRandomUtil.nextBytes(16)),
        Collections.emptyList(),
        aciIdentityKey,
        receiptPresentation(receiptSerial, CLOCK.instant().plus(Duration.ofDays(30)), 1),
        new DeviceSpec(null,
            password,
            "OWI",
            Set.of(),
            new DeviceIdentityInfo(1, KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
            Optional.empty(),
            true,
            Optional.empty(),
            Optional.empty()),
        null));
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void assertExpectedStoredAccount(final Account account,
      final Optional<String> number,
      final String password,
      final String signalAgent,
      final DeliveryChannels deliveryChannels,
      final int registrationId,
      final Optional<Integer> pniRegistrationId,
      final byte[] deviceName,
      final boolean discoverableByPhoneNumber,
      final Set<DeviceCapability> deviceCapabilities,
      final List<AccountBadge> badges,
      final Optional<ApnRegistrationId> maybeApnRegistrationId,
      final Optional<GcmRegistrationId> maybeGcmRegistrationId,
      final Optional<String> registrationLockSecret,
      final ECSignedPreKey aciSignedPreKey,
      final Optional<ECSignedPreKey> pniSignedPreKey,
      final KEMSignedPreKey aciPqLastResortPreKey,
      final Optional<KEMSignedPreKey> pniPqLastResortPreKey) {

    final Device primaryDevice = account.getPrimaryDevice();

    assertEquals(number, account.getNumberOptional());
    assertEquals(signalAgent, primaryDevice.getUserAgent());
    assertEquals(deliveryChannels.fetchesMessages(), primaryDevice.getFetchesMessages());
    assertEquals(registrationId, primaryDevice.getAccountRegistrationId());
    assertEquals(pniRegistrationId, primaryDevice.getPhoneNumberIdentityRegistrationId());
    assertArrayEquals(deviceName, primaryDevice.getName());
    assertEquals(discoverableByPhoneNumber, account.isDiscoverableByPhoneNumber());
    assertEquals(deviceCapabilities, primaryDevice.getCapabilities());
    assertEquals(badges, account.getBadges());

    maybeApnRegistrationId.ifPresentOrElse(
        apnRegistrationId -> assertEquals(apnRegistrationId.apnRegistrationId(), primaryDevice.getApnId()),
        () -> assertNull(primaryDevice.getApnId()));

    maybeGcmRegistrationId.ifPresentOrElse(
        _ -> assertEquals(deliveryChannels.fcmToken(), primaryDevice.getGcmId()),
        () -> assertNull(primaryDevice.getGcmId()));

    registrationLockSecret.ifPresent(regLockSecret -> assertTrue(account.getRegistrationLock().verify(regLockSecret)));
    assertTrue(primaryDevice.getAuthTokenHash().verify(password));
    assertNotNull(primaryDevice.getCreatedAtCiphertext());
    assertEquals(Optional.of(aciSignedPreKey), keysManager.getEcSignedPreKey(account.getAccountIdentifier(), Device.PRIMARY_ID).join());
    assertEquals(Optional.of(aciPqLastResortPreKey), keysManager.getLastResort(account.getAccountIdentifier(), Device.PRIMARY_ID).join());
    account.getPhoneNumberIdentifierOptional().ifPresent(
        pni -> {
          assertEquals(pniSignedPreKey, keysManager.getEcSignedPreKey(pni, Device.PRIMARY_ID).join());
          assertEquals(pniPqLastResortPreKey, keysManager.getLastResort(pni, Device.PRIMARY_ID).join());
        });
    assertEquals(number.isEmpty(), account.getAuthCredentialSalt().isPresent());
  }
}
