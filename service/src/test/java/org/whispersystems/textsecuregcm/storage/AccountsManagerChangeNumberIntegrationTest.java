/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

class AccountsManagerChangeNumberIntegrationTest {

  private static final int SCAN_PAGE_SIZE = 1;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.ACCOUNTS,
      Tables.DELETED_ACCOUNTS,
      Tables.DELETED_ACCOUNTS_LOCK,
      Tables.NUMBERS,
      Tables.PNI,
      Tables.PNI_ASSIGNMENTS,
      Tables.USERNAMES);

  @RegisterExtension
  static final RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private ClientPresenceManager clientPresenceManager;
  private DeletedAccounts deletedAccounts;

  private AccountsManager accountsManager;

  @BeforeEach
  void setup() throws InterruptedException {

    {
      @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
          mock(DynamicConfigurationManager.class);

      DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
      when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

      final Accounts accounts = new Accounts(
          DYNAMO_DB_EXTENSION.getDynamoDbClient(),
          DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
          Tables.ACCOUNTS.tableName(),
          Tables.NUMBERS.tableName(),
          Tables.PNI_ASSIGNMENTS.tableName(),
          Tables.USERNAMES.tableName(),
          SCAN_PAGE_SIZE);

      deletedAccounts = new DeletedAccounts(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
          Tables.DELETED_ACCOUNTS.tableName());

      final DeletedAccountsManager deletedAccountsManager = new DeletedAccountsManager(deletedAccounts,
          DYNAMO_DB_EXTENSION.getLegacyDynamoClient(),
          Tables.DELETED_ACCOUNTS_LOCK.tableName());

      final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
      when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

      final SecureBackupClient secureBackupClient = mock(SecureBackupClient.class);
      when(secureBackupClient.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

      final SecureValueRecovery2Client svr2Client = mock(SecureValueRecovery2Client.class);
      when(svr2Client.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

      clientPresenceManager = mock(ClientPresenceManager.class);

      final PhoneNumberIdentifiers phoneNumberIdentifiers =
          new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbClient(), Tables.PNI.tableName());

      accountsManager = new AccountsManager(
          accounts,
          phoneNumberIdentifiers,
          CACHE_CLUSTER_EXTENSION.getRedisCluster(),
          deletedAccountsManager,
          mock(Keys.class),
          mock(MessagesManager.class),
          mock(ProfilesManager.class),
          mock(StoredVerificationCodeManager.class),
          secureStorageClient,
          secureBackupClient,
          svr2Client,
          clientPresenceManager,
          mock(ExperimentEnrollmentManager.class),
          mock(RegistrationRecoveryPasswordsManager.class),
          mock(Clock.class));
    }
  }

  @Test
  void testChangeNumber() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    accountsManager.changeNumber(account, secondNumber, null, null, null, null);

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    assertTrue(accountsManager.getByE164(secondNumber).isPresent());
    assertEquals(originalUuid, accountsManager.getByE164(secondNumber).map(Account::getUuid).orElseThrow());
    assertNotEquals(originalPni, accountsManager.getByE164(secondNumber).map(Account::getPhoneNumberIdentifier).orElseThrow());

    assertEquals(secondNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
  }

  @Test
  void testChangeNumberWithPniExtensions() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";
    final int rotatedPniRegistrationId = 17;
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final SignedPreKey rotatedSignedPreKey = KeysHelper.signedECPreKey(1L, pniIdentityKeyPair);

    final AccountAttributes accountAttributes = new AccountAttributes(true, rotatedPniRegistrationId + 1, "test", null, true, new Device.DeviceCapabilities());
    final Account account = accountsManager.create(originalNumber, "password", null, accountAttributes, new ArrayList<>());
    account.getMasterDevice().orElseThrow().setSignedPreKey(new SignedPreKey());

    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final byte[] pniIdentityKey = pniIdentityKeyPair.getPublicKey().serialize();
    final Map<Long, SignedPreKey> preKeys = Map.of(Device.MASTER_ID, rotatedSignedPreKey);
    final Map<Long, Integer> registrationIds = Map.of(Device.MASTER_ID, rotatedPniRegistrationId);

    final Account updatedAccount = accountsManager.changeNumber(account, secondNumber, pniIdentityKey, preKeys, null, registrationIds);

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    assertTrue(accountsManager.getByE164(secondNumber).isPresent());
    assertEquals(originalUuid, accountsManager.getByE164(secondNumber).map(Account::getUuid).orElseThrow());
    assertNotEquals(originalPni, accountsManager.getByE164(secondNumber).map(Account::getPhoneNumberIdentifier).orElseThrow());

    assertEquals(secondNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));

    assertArrayEquals(pniIdentityKey, updatedAccount.getPhoneNumberIdentityKey());

    assertEquals(OptionalInt.of(rotatedPniRegistrationId),
        updatedAccount.getMasterDevice().orElseThrow().getPhoneNumberIdentityRegistrationId());

    assertEquals(rotatedSignedPreKey, updatedAccount.getMasterDevice().orElseThrow().getPhoneNumberIdentitySignedPreKey());
  }

  @Test
  void testChangeNumberReturnToOriginal() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    account = accountsManager.changeNumber(account, secondNumber, null, null, null, null);
    accountsManager.changeNumber(account, originalNumber, null, null, null, null);

    assertTrue(accountsManager.getByE164(originalNumber).isPresent());
    assertEquals(originalUuid, accountsManager.getByE164(originalNumber).map(Account::getUuid).orElseThrow());
    assertEquals(originalPni, accountsManager.getByE164(originalNumber).map(Account::getPhoneNumberIdentifier).orElseThrow());

    assertTrue(accountsManager.getByE164(secondNumber).isEmpty());

    assertEquals(originalNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
  }

  @Test
  void testChangeNumberContested() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();

    final Account existingAccount = accountsManager.create(secondNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID existingAccountUuid = existingAccount.getUuid();

    accountsManager.changeNumber(account, secondNumber, null, null, null, null);

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    assertTrue(accountsManager.getByE164(secondNumber).isPresent());
    assertEquals(Optional.of(originalUuid), accountsManager.getByE164(secondNumber).map(Account::getUuid));

    assertEquals(secondNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    verify(clientPresenceManager).disconnectPresence(existingAccountUuid, Device.MASTER_ID);

    assertEquals(Optional.of(existingAccountUuid), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));

    accountsManager.changeNumber(accountsManager.getByAccountIdentifier(originalUuid).orElseThrow(), originalNumber, null, null, null, null);

    final Account existingAccount2 = accountsManager.create(secondNumber, "password", null, new AccountAttributes(),
        new ArrayList<>());

    assertEquals(existingAccountUuid, existingAccount2.getUuid());
  }

  @Test
  void testChangeNumberChaining() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final Account existingAccount = accountsManager.create(secondNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID existingAccountUuid = existingAccount.getUuid();

    final Account changedNumberAccount = accountsManager.changeNumber(account, secondNumber, null, null, null, null);
    final UUID secondPni = changedNumberAccount.getPhoneNumberIdentifier();

    final Account reRegisteredAccount = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());

    assertEquals(existingAccountUuid, reRegisteredAccount.getUuid());
    assertEquals(originalPni, reRegisteredAccount.getPhoneNumberIdentifier());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));

    final Account changedNumberReRegisteredAccount = accountsManager.changeNumber(reRegisteredAccount, secondNumber, null, null, null, null);

    assertEquals(Optional.of(originalUuid), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
    assertEquals(secondPni, changedNumberReRegisteredAccount.getPhoneNumberIdentifier());
  }
}
