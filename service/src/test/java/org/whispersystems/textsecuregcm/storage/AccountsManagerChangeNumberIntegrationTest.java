/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

class AccountsManagerChangeNumberIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.ACCOUNTS,
      Tables.CLIENT_PUBLIC_KEYS,
      Tables.DELETED_ACCOUNTS,
      Tables.DELETED_ACCOUNTS_LOCK,
      Tables.NUMBERS,
      Tables.PNI,
      Tables.PNI_ASSIGNMENTS,
      Tables.USERNAMES,
      Tables.EC_KEYS,
      Tables.PQ_KEYS,
      Tables.PAGED_PQ_KEYS,
      Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS);

  @RegisterExtension
  static final RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension("testbucket");

  private KeysManager keysManager;
  private DisconnectionRequestManager disconnectionRequestManager;
  private ScheduledExecutorService executor;

  private AccountsManager accountsManager;

  @BeforeEach
  void setup() throws InterruptedException {

    {
      @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
          mock(DynamicConfigurationManager.class);

      DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
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
          Clock.systemUTC(),
          DYNAMO_DB_EXTENSION.getDynamoDbClient(),
          DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
          Tables.ACCOUNTS.tableName(),
          Tables.NUMBERS.tableName(),
          Tables.PNI_ASSIGNMENTS.tableName(),
          Tables.USERNAMES.tableName(),
          Tables.DELETED_ACCOUNTS.tableName(),
          Tables.USED_LINK_DEVICE_TOKENS.tableName());

      executor = Executors.newSingleThreadScheduledExecutor();

      final AccountLockManager accountLockManager = new AccountLockManager(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
          Tables.DELETED_ACCOUNTS_LOCK.tableName());

      final ClientPublicKeysManager clientPublicKeysManager =
          new ClientPublicKeysManager(clientPublicKeys, accountLockManager, executor);

      final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
      when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

      final SecureValueRecoveryClient svr2Client = mock(SecureValueRecoveryClient.class);
      when(svr2Client.removeData(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));

      disconnectionRequestManager = mock(DisconnectionRequestManager.class);

      final PhoneNumberIdentifiers phoneNumberIdentifiers =
          new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.PNI.tableName());

      final MessagesManager messagesManager = mock(MessagesManager.class);
      when(messagesManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));

      final ProfilesManager profilesManager = mock(ProfilesManager.class);
      when(profilesManager.deleteAll(any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
          mock(RegistrationRecoveryPasswordsManager.class);

      when(registrationRecoveryPasswordsManager.remove(any()))
          .thenReturn(CompletableFuture.completedFuture(null));

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
          executor,
          mock(Clock.class),
          "link-device-secret".getBytes(StandardCharsets.UTF_8),
          dynamicConfigurationManager);
    }
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdown();

    //noinspection ResultOfMethodCallIgnored
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void testChangeNumber() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";
    final Account account = AccountsHelper.createAccount(accountsManager, originalNumber);

    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    accountsManager.changeNumber(account,
        secondNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    final Account updatedAccount = accountsManager.getByE164(secondNumber).orElseThrow();
    assertEquals(originalUuid, updatedAccount.getUuid());
    assertEquals(secondNumber, updatedAccount.getNumber());
    assertNotEquals(originalPni, updatedAccount.getPhoneNumberIdentifier());

    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(originalPni));
    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(updatedAccount.getPhoneNumberIdentifier()));
  }

  @Test
  void testChangeNumberSameNumber() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final Account account = AccountsHelper.createAccount(accountsManager, originalNumber);

    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    accountsManager.changeNumber(account,
        originalNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    final Account updatedAccount = accountsManager.getByE164(originalNumber).orElseThrow();
    assertEquals(originalUuid, updatedAccount.getUuid());
    assertEquals(originalNumber, updatedAccount.getNumber());
    assertEquals(originalPni, updatedAccount.getPhoneNumberIdentifier());

    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(originalPni));
    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(updatedAccount.getPhoneNumberIdentifier()));
  }

  @Test
  void testChangeNumberWithPniExtensions() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";
    final int rotatedPniRegistrationId = 17;
    final ECKeyPair rotatedPniIdentityKeyPair = ECKeyPair.generate();
    final ECSignedPreKey rotatedSignedPreKey = KeysHelper.signedECPreKey(1L, rotatedPniIdentityKeyPair);
    final KEMSignedPreKey rotatedKemSignedPreKey = KeysHelper.signedKEMPreKey(2L, rotatedPniIdentityKeyPair);
    final AccountAttributes accountAttributes = new AccountAttributes(true, rotatedPniRegistrationId + 1, rotatedPniRegistrationId, "test".getBytes(StandardCharsets.UTF_8), null, true, Set.of());
    final Account account = AccountsHelper.createAccount(accountsManager, originalNumber, accountAttributes);

    keysManager.storeEcSignedPreKeys(account.getIdentifier(IdentityType.ACI),
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, rotatedPniIdentityKeyPair)).join();

    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final IdentityKey pniIdentityKey = new IdentityKey(rotatedPniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> preKeys = Map.of(Device.PRIMARY_ID, rotatedSignedPreKey);
    final Map<Byte, KEMSignedPreKey> kemSignedPreKeys = Map.of(Device.PRIMARY_ID, rotatedKemSignedPreKey);
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, rotatedPniRegistrationId);

    final Account updatedAccount = accountsManager.changeNumber(account, secondNumber, pniIdentityKey, preKeys, kemSignedPreKeys, registrationIds);
    final UUID secondPni = updatedAccount.getPhoneNumberIdentifier();

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    assertTrue(accountsManager.getByE164(secondNumber).isPresent());
    assertEquals(originalUuid, accountsManager.getByE164(secondNumber).map(Account::getUuid).orElseThrow());
    assertNotEquals(originalPni, secondPni);
    assertEquals(secondPni, accountsManager.getByE164(secondNumber).map(Account::getPhoneNumberIdentifier).orElseThrow());

    assertEquals(secondNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(originalPni));
    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(secondPni));

    assertEquals(pniIdentityKey, updatedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(rotatedPniRegistrationId, updatedAccount.getPrimaryDevice().getRegistrationId(IdentityType.PNI));

    assertEquals(Optional.of(rotatedSignedPreKey),
        keysManager.getEcSignedPreKey(updatedAccount.getIdentifier(IdentityType.PNI), Device.PRIMARY_ID).join());
  }

  @Test
  void testChangeNumberReturnToOriginal() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    Account account = AccountsHelper.createAccount(accountsManager, originalNumber);

    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final ECKeyPair originalIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair secondIdentityKeyPair = ECKeyPair.generate();

    account = accountsManager.changeNumber(account,
        secondNumber,
        new IdentityKey(secondIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, secondIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, secondIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    final UUID secondPni = account.getPhoneNumberIdentifier();

    accountsManager.changeNumber(account,
        originalNumber,
        new IdentityKey(originalIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(3, originalIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(4, originalIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 2));

    assertTrue(accountsManager.getByE164(originalNumber).isPresent());
    assertEquals(originalUuid, accountsManager.getByE164(originalNumber).map(Account::getUuid).orElseThrow());
    assertEquals(originalPni, accountsManager.getByE164(originalNumber).map(Account::getPhoneNumberIdentifier).orElseThrow());

    assertTrue(accountsManager.getByE164(secondNumber).isEmpty());

    assertEquals(originalNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(originalPni));
    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(secondPni));
  }

  @Test
  void testChangeNumberContested() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = AccountsHelper.createAccount(accountsManager, originalNumber);

    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final ECKeyPair originalIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair secondIdentityKeyPair = ECKeyPair.generate();

    final Account existingAccount = AccountsHelper.createAccount(accountsManager, secondNumber);

    final UUID existingAccountUuid = existingAccount.getUuid();

    accountsManager.changeNumber(account,
        secondNumber,
        new IdentityKey(secondIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, secondIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, secondIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    final UUID secondPni = accountsManager.getByE164(secondNumber).get().getPhoneNumberIdentifier();

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    assertTrue(accountsManager.getByE164(secondNumber).isPresent());
    assertEquals(Optional.of(originalUuid), accountsManager.getByE164(secondNumber).map(Account::getUuid));

    assertEquals(secondNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    verify(disconnectionRequestManager).requestDisconnection(argThat(disconnectedAccount ->
        disconnectedAccount.getIdentifier(IdentityType.ACI).equals(existingAccountUuid) && disconnectedAccount != account));

    assertEquals(Optional.of(existingAccountUuid), accountsManager.findRecentlyDeletedAccountIdentifier(originalPni));
    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(secondPni));

    accountsManager.changeNumber(accountsManager.getByAccountIdentifier(originalUuid).orElseThrow(),
        originalNumber,
        new IdentityKey(originalIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, originalIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, originalIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    final Account existingAccount2 = AccountsHelper.createAccount(accountsManager, secondNumber);

    assertEquals(existingAccountUuid, existingAccount2.getUuid());
  }

  @Test
  void testChangeNumberChaining() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = AccountsHelper.createAccount(accountsManager, originalNumber);

    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final Account existingAccount = AccountsHelper.createAccount(accountsManager, secondNumber);

    final UUID existingAccountUuid = existingAccount.getUuid();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final Account changedNumberAccount = accountsManager.changeNumber(account,
        secondNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    final UUID secondPni = changedNumberAccount.getPhoneNumberIdentifier();

    final Account reRegisteredAccount = AccountsHelper.createAccount(accountsManager, originalNumber);

    assertEquals(existingAccountUuid, reRegisteredAccount.getUuid());
    assertEquals(originalPni, reRegisteredAccount.getPhoneNumberIdentifier());

    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(originalPni));
    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(secondPni));

    final ECKeyPair reRegisteredPniIdentityKeyPair = ECKeyPair.generate();

    final Account changedNumberReRegisteredAccount = accountsManager.changeNumber(reRegisteredAccount,
        secondNumber,
        new IdentityKey(reRegisteredPniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, reRegisteredPniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, reRegisteredPniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    assertEquals(Optional.of(originalUuid), accountsManager.findRecentlyDeletedAccountIdentifier(originalPni));
    assertEquals(Optional.empty(), accountsManager.findRecentlyDeletedAccountIdentifier(secondPni));
    assertEquals(secondPni, changedNumberReRegisteredAccount.getPhoneNumberIdentifier());
  }
}
