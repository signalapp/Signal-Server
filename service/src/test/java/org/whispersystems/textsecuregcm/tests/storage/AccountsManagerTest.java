/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicAccountsDynamoDbMigrationConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsDynamoDb;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ContestedOptimisticLockException;
import org.whispersystems.textsecuregcm.storage.DeletedAccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.tests.util.JsonHelpers;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

class AccountsManagerTest {

  private Accounts accounts;
  private AccountsDynamoDb accountsDynamoDb;
  private DeletedAccountsManager deletedAccountsManager;
  private DirectoryQueue directoryQueue;
  private DynamicConfigurationManager dynamicConfigurationManager;
  private ExperimentEnrollmentManager experimentEnrollmentManager;
  private KeysDynamoDb keys;
  private MessagesManager messagesManager;
  private ProfilesManager profilesManager;

  private RedisAdvancedClusterCommands<String, String> commands;
  private AccountsManager accountsManager;

  private static final Answer<?> ACCOUNT_UPDATE_ANSWER = (answer) -> {
    // it is implicit in the update() contract is that a successful call will
    // result in an incremented version
    final Account updatedAccount = answer.getArgument(0, Account.class);
    updatedAccount.setVersion(updatedAccount.getVersion() + 1);
    return null;
  };

  @BeforeEach
  void setup() throws InterruptedException {
    accounts = mock(Accounts.class);
    accountsDynamoDb = mock(AccountsDynamoDb.class);
    deletedAccountsManager = mock(DeletedAccountsManager.class);
    directoryQueue = mock(DirectoryQueue.class);
    dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);
    keys = mock(KeysDynamoDb.class);
    messagesManager = mock(MessagesManager.class);
    profilesManager = mock(ProfilesManager.class);

    //noinspection unchecked
    commands = mock(RedisAdvancedClusterCommands.class);

    final DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    doAnswer(invocation -> {
      //noinspection unchecked
      invocation.getArgument(1, Consumer.class).accept(Optional.empty());
      return null;
    }).when(deletedAccountsManager).lockAndTake(anyString(), any());

    accountsManager = new AccountsManager(
        accounts,
        accountsDynamoDb,
        RedisClusterHelper.buildMockRedisCluster(commands),
        deletedAccountsManager,
        directoryQueue,
        keys,
        messagesManager,
        mock(UsernamesManager.class),
        profilesManager,
        mock(StoredVerificationCodeManager.class),
        mock(SecureStorageClient.class),
        mock(SecureBackupClient.class),
        experimentEnrollmentManager,
        dynamicConfigurationManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByNumberInCache(final boolean dynamoEnabled) {
    UUID uuid = UUID.randomUUID();

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    Optional<Account> account = accountsManager.get("+14152222222");

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getProfileName(), "test");

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(accounts);

    verifyNoInteractions(accountsDynamoDb);
  }

  private void enableDynamo(boolean dynamoEnabled) {
    final DynamicAccountsDynamoDbMigrationConfiguration config = dynamicConfigurationManager.getConfiguration()
        .getAccountsDynamoDbMigrationConfiguration();

    config.setDeleteEnabled(dynamoEnabled);
    config.setReadEnabled(dynamoEnabled);
    config.setWriteEnabled(dynamoEnabled);

    when(experimentEnrollmentManager.isEnrolled(any(UUID.class), anyString()))
        .thenReturn(dynamoEnabled);

  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidInCache(boolean dynamoEnabled) {
    UUID uuid = UUID.randomUUID();

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    Optional<Account> account = accountsManager.get(uuid);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(account.get().getProfileName(), "test");

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(accounts);

    verifyNoInteractions(accountsDynamoDb);
  }


  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByNumberNotInCache(boolean dynamoEnabled) {
    UUID uuid = UUID.randomUUID();
    Account account = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(null);
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never())
        .get(eq("+14152222222"));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidNotInCache(boolean dynamoEnabled) {
    UUID uuid = UUID.randomUUID();
    Account account = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).get(eq(uuid));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByNumberBrokenCache(boolean dynamoEnabled) {
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("AccountMap::+14152222222"))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    Optional<Account> retrieved       = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).get(eq("+14152222222"));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidBrokenCache(boolean dynamoEnabled) {
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved       = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).get(eq(uuid));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdate_dynamoDbMigration(boolean dynamoEnabled) throws IOException {
    UUID uuid = UUID.randomUUID();
    Account account = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    // database fetches should always return new instances
    when(accounts.get(uuid)).thenReturn(Optional.of(new Account("+14152222222", uuid, new HashSet<>(), new byte[16])));
    when(accountsDynamoDb.get(uuid)).thenReturn(Optional.of(new Account("+14152222222", uuid, new HashSet<>(), new byte[16])));
    doAnswer(ACCOUNT_UPDATE_ANSWER).when(accounts).update(any(Account.class));

    Account updatedAccount = accountsManager.update(account, a -> a.setProfileName("name"));

    assertThrows(AssertionError.class, account::getProfileName, "Account passed to update() should be stale");

    assertNotSame(updatedAccount, account);

    verify(accounts, times(1)).update(account);
    verifyNoMoreInteractions(accounts);

    if (dynamoEnabled) {
      ArgumentCaptor<Account> argumentCaptor = ArgumentCaptor.forClass(Account.class);
      verify(accountsDynamoDb, times(1)).update(argumentCaptor.capture());
      assertEquals(uuid, argumentCaptor.getValue().getUuid());
    } else {
      verify(accountsDynamoDb, never()).update(any());
    }
    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).get(uuid);
    verifyNoMoreInteractions(accountsDynamoDb);

    ArgumentCaptor<String> redisSetArgumentCapture = ArgumentCaptor.forClass(String.class);

    verify(commands, times(2)).set(anyString(), redisSetArgumentCapture.capture());

    Account accountCached = JsonHelpers.fromJson(redisSetArgumentCapture.getAllValues().get(1), Account.class);

    // uuid is @JsonIgnore, so we need to set it for compareAccounts to work
    accountCached.setUuid(uuid);

    assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(updatedAccount), Optional.of(accountCached)));
  }

  @Test
  void testUpdate_dynamoMissing() {
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(true);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accountsDynamoDb.get(uuid)).thenReturn(Optional.empty());
    doAnswer(ACCOUNT_UPDATE_ANSWER).when(accounts).update(any());
    doAnswer(ACCOUNT_UPDATE_ANSWER).when(accountsDynamoDb).update(any());

    Account updatedAccount = accountsManager.update(account,  a -> {});

    verify(accounts, times(1)).update(account);
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, never()).update(account);
    verify(accountsDynamoDb, times(1)).get(uuid);
    verifyNoMoreInteractions(accountsDynamoDb);

    assertEquals(1, updatedAccount.getVersion());
  }

  @Test
  void testUpdate_optimisticLockingFailure() {
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(true);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.get(uuid)).thenReturn(Optional.of(new Account("+14152222222", uuid, new HashSet<>(), new byte[16])));
    doThrow(ContestedOptimisticLockException.class)
        .doAnswer(ACCOUNT_UPDATE_ANSWER)
        .when(accounts).update(any());

    when(accountsDynamoDb.get(uuid)).thenReturn(Optional.of(new Account("+14152222222", uuid, new HashSet<>(), new byte[16])));
    doThrow(ContestedOptimisticLockException.class)
        .doAnswer(ACCOUNT_UPDATE_ANSWER)
        .when(accountsDynamoDb).update(any());

    account = accountsManager.update(account, a -> a.setProfileName("name"));

    assertEquals(1, account.getVersion());
    assertEquals("name", account.getProfileName());

    verify(accounts, times(1)).get(uuid);
    verify(accounts, times(2)).update(any());
    verifyNoMoreInteractions(accounts);

    // dynamo has an extra get() because the account is fetched before every update
    verify(accountsDynamoDb, times(2)).get(uuid);
    verify(accountsDynamoDb, times(2)).update(any());
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @Test
  void testUpdate_dynamoOptimisticLockingFailureDuringCreate() {
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(true);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accountsDynamoDb.get(uuid)).thenReturn(Optional.empty())
                                    .thenReturn(Optional.of(account));
    when(accountsDynamoDb.create(any())).thenThrow(ContestedOptimisticLockException.class);

    accountsManager.update(account, a -> {});

    verify(accounts, times(1)).update(account);
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, times(1)).get(uuid);
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @Test
  void testUpdateDevice() {
    assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.empty(), Optional.empty()));

    final UUID uuid = UUID.randomUUID();
    Account account = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(accounts.get(uuid)).thenReturn(Optional.of(new Account("+14152222222", uuid, new HashSet<>(), new byte[16])));

    assertTrue(account.getDevices().isEmpty());

    Device enabledDevice = new Device();
    enabledDevice.setFetchesMessages(true);
    enabledDevice.setSignedPreKey(new SignedPreKey(1L, "key", "signature"));
    enabledDevice.setLastSeen(System.currentTimeMillis());
    final long deviceId = account.getNextDeviceId();
    enabledDevice.setId(deviceId);
    account.addDevice(enabledDevice);

    @SuppressWarnings("unchecked") Consumer<Device> deviceUpdater = mock(Consumer.class);
    @SuppressWarnings("unchecked") Consumer<Device> unknownDeviceUpdater = mock(Consumer.class);

    account = accountsManager.updateDevice(account, deviceId, deviceUpdater);
    account = accountsManager.updateDevice(account, deviceId, d -> d.setName("deviceName"));

    assertEquals("deviceName", account.getDevice(deviceId).orElseThrow().getName());

    verify(deviceUpdater, times(1)).accept(any(Device.class));

    accountsManager.updateDevice(account, account.getNextDeviceId(), unknownDeviceUpdater);

    verify(unknownDeviceUpdater, never()).accept(any(Device.class));
  }

  @Test
  void testCompareAccounts() throws Exception {
    assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.empty(), Optional.empty()));

    final UUID uuidA = UUID.randomUUID();
    final Account a1 = new Account("+14152222222", uuidA, new HashSet<>(), new byte[16]);

    assertEquals(Optional.of("dbMissing"), accountsManager.compareAccounts(Optional.empty(), Optional.of(a1)));

    final Account a2 = new Account("+14152222222", uuidA, new HashSet<>(), new byte[16]);

    assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

    {
      Device device1 = new Device();
      device1.setId(1L);

      a1.addDevice(device1);

      assertEquals(Optional.of("devices"), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

      Device device2 = new Device();
      device2.setId(1L);

      a2.addDevice(device2);

      assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

      device1.setLastSeen(1L);

      assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

      device1.setName("name");

      assertEquals(Optional.of("devices"), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

      device1.setName(null);

      device1.setSignedPreKey(new SignedPreKey(1L, "123", "456"));
      device2.setSignedPreKey(new SignedPreKey(2L, "123", "456"));

      assertEquals(Optional.of("masterDeviceSignedPreKey"), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

      device1.setSignedPreKey(null);
      device2.setSignedPreKey(null);

      assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

      device1.setApnId("123");
      Thread.sleep(5);
      device2.setApnId("123");

      assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

      a1.removeDevice(1L);
      a2.removeDevice(1L);

      assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));
    }

    assertEquals(Optional.empty(), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

    a1.setVersion(1);

    assertEquals(Optional.of("version"), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

    a2.setVersion(1);

    a2.setProfileName("name");

    assertEquals(Optional.of("profileName"), accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));
  }

  @Test
  void testCreateFreshAccount() throws InterruptedException {
    when(accounts.create(any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true, null);
    accountsManager.create(e164, "password", null, attributes);

    verify(accounts).create(argThat(account -> e164.equals(account.getNumber())));
    verifyNoInteractions(keys);
    verifyNoInteractions(messagesManager);
    verifyNoInteractions(profilesManager);
  }

  @Test
  void testReregisterAccount() throws InterruptedException {
    final UUID existingUuid = UUID.randomUUID();

    when(accounts.create(any())).thenAnswer(invocation -> {
      invocation.getArgument(0, Account.class).setUuid(existingUuid);
      return false;
    });

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true, null);
    accountsManager.create(e164, "password", null, attributes);

    verify(accounts).create(argThat(account -> e164.equals(account.getNumber()) && existingUuid.equals(account.getUuid())));
    verify(keys).delete(existingUuid);
    verify(messagesManager).clear(existingUuid);
    verify(profilesManager).deleteAll(existingUuid);
  }

  @Test
  void testCreateAccountRecentlyDeleted() throws InterruptedException {
    final UUID recentlyDeletedUuid = UUID.randomUUID();

    doAnswer(invocation -> {
      //noinspection unchecked
      invocation.getArgument(1, Consumer.class).accept(Optional.of(recentlyDeletedUuid));
      return null;
    }).when(deletedAccountsManager).lockAndTake(anyString(), any());

    when(accounts.create(any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true, null);
    accountsManager.create(e164, "password", null, attributes);

    verify(accounts).create(argThat(account -> e164.equals(account.getNumber()) && recentlyDeletedUuid.equals(account.getUuid())));
    verifyNoInteractions(keys);
    verifyNoInteractions(messagesManager);
    verifyNoInteractions(profilesManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithDiscoverability(final boolean discoverable) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, discoverable, null);
    final Account account = accountsManager.create("+18005550123", "password", null, attributes);

    assertEquals(discoverable, account.isDiscoverableByPhoneNumber());

    if (!discoverable) {
      verify(directoryQueue).deleteAccount(account);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithStorageCapability(final boolean hasStorage) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true,
        new DeviceCapabilities(false, false, false, hasStorage, false, false, false, false));

    final Account account = accountsManager.create("+18005550123", "password", null, attributes);

    assertEquals(hasStorage, account.isStorageSupported());
  }

  @ParameterizedTest
  @MethodSource
  void testUpdateDirectoryQueue(final boolean visibleBeforeUpdate, final boolean visibleAfterUpdate,
      final boolean expectRefresh) {
    final Account account = new Account("+14152222222", UUID.randomUUID(), new HashSet<>(), new byte[16]);

    // this sets up the appropriate result for Account#shouldBeVisibleInDirectory
    final Device device = new Device(Device.MASTER_ID, "device", "token", "salt", null, null, null, true, 1,
        new SignedPreKey(1, "key", "sig"), 0, 0,
        "OWT", 0, new DeviceCapabilities());
    account.addDevice(device);
    account.setDiscoverableByPhoneNumber(visibleBeforeUpdate);

    final Account updatedAccount = accountsManager.update(account,
        a -> a.setDiscoverableByPhoneNumber(visibleAfterUpdate));

    verify(directoryQueue, times(expectRefresh ? 1 : 0)).refreshAccount(updatedAccount);
  }

  private static Stream<Arguments> testUpdateDirectoryQueue() {
    return Stream.of(
        Arguments.of(false, false, false),
        Arguments.of(true, true, false),
        Arguments.of(false, true, true),
        Arguments.of(true, false, true));
  }

  @ParameterizedTest
  @MethodSource
  void testUpdateDeviceLastSeen(final boolean expectUpdate, final long initialLastSeen, final long updatedLastSeen) {
    final Account account = new Account("+14152222222", UUID.randomUUID(), new HashSet<>(), new byte[16]);
    final Device device = new Device(Device.MASTER_ID, "device", "token", "salt", null, null, null, true, 1,
        new SignedPreKey(1, "key", "sig"), initialLastSeen, 0,
        "OWT", 0, new DeviceCapabilities());
    account.addDevice(device);

    accountsManager.updateDeviceLastSeen(account, device, updatedLastSeen);

    assertEquals(expectUpdate ? updatedLastSeen : initialLastSeen, device.getLastSeen());
    verify(accounts, expectUpdate ? times(1) : never()).update(account);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> testUpdateDeviceLastSeen() {
    return Stream.of(
        Arguments.of(true, 1, 2),
        Arguments.of(false, 1, 1),
        Arguments.of(false, 2, 1)
    );
  }
}
