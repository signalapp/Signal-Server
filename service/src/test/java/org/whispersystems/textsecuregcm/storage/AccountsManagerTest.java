/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
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
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.configuration.UsernameConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.UsernameGenerator;
import org.whispersystems.textsecuregcm.util.UsernameNormalizer;

class AccountsManagerTest {

  private Accounts accounts;
  private DeletedAccountsManager deletedAccountsManager;
  private DirectoryQueue directoryQueue;
  private Keys keys;
  private MessagesManager messagesManager;
  private ProfilesManager profilesManager;
  private ProhibitedUsernames prohibitedUsernames;
  private ExperimentEnrollmentManager enrollmentManager;

  private Map<String, UUID> phoneNumberIdentifiersByE164;

  private RedisAdvancedClusterCommands<String, String> commands;
  private AccountsManager accountsManager;

  private static final Answer<?> ACCOUNT_UPDATE_ANSWER = (answer) -> {
    // it is implicit in the update() contract is that a successful call will
    // result in an incremented version
    final Account updatedAccount = answer.getArgument(0, Account.class);
    updatedAccount.setVersion(updatedAccount.getVersion() + 1);
    return null;
  };

  private static final UUID RESERVATION_TOKEN = UUID.randomUUID();

  @BeforeEach
  void setup() throws InterruptedException {
    accounts = mock(Accounts.class);
    deletedAccountsManager = mock(DeletedAccountsManager.class);
    directoryQueue = mock(DirectoryQueue.class);
    keys = mock(Keys.class);
    messagesManager = mock(MessagesManager.class);
    profilesManager = mock(ProfilesManager.class);
    prohibitedUsernames = mock(ProhibitedUsernames.class);

    //noinspection unchecked
    commands = mock(RedisAdvancedClusterCommands.class);

    doAnswer((Answer<Void>) invocation -> {
      final Account account = invocation.getArgument(0, Account.class);
      final String number = invocation.getArgument(1, String.class);
      final UUID phoneNumberIdentifier = invocation.getArgument(2, UUID.class);

      account.setNumber(number, phoneNumberIdentifier);

      return null;
    }).when(accounts).changeNumber(any(), anyString(), any());

    doAnswer(invocation -> {
      //noinspection unchecked
      invocation.getArgument(1, Consumer.class).accept(Optional.empty());
      return null;
    }).when(deletedAccountsManager).lockAndTake(anyString(), any());

    final SecureStorageClient storageClient = mock(SecureStorageClient.class);
    when(storageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureBackupClient backupClient = mock(SecureBackupClient.class);
    when(backupClient.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    phoneNumberIdentifiersByE164 = new HashMap<>();

    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString())).thenAnswer((Answer<UUID>) invocation -> {
      final String number = invocation.getArgument(0, String.class);
      return phoneNumberIdentifiersByE164.computeIfAbsent(number, n -> UUID.randomUUID());
    });

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    enrollmentManager = mock(ExperimentEnrollmentManager.class);
    when(enrollmentManager.isEnrolled(any(UUID.class), eq(AccountsManager.USERNAME_EXPERIMENT_NAME))).thenReturn(true);
    when(accounts.usernameAvailable(any())).thenReturn(true);

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        RedisClusterHelper.builder().stringCommands(commands).build(),
        deletedAccountsManager,
        directoryQueue,
        keys,
        messagesManager,
        prohibitedUsernames,
        profilesManager,
        mock(StoredVerificationCodeManager.class),
        storageClient,
        backupClient,
        mock(ClientPresenceManager.class),
        new UsernameGenerator(new UsernameConfiguration()),
        enrollmentManager,
        mock(Clock.class));
  }

  @Test
  void testGetAccountByNumberInCache() {
    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid))).thenReturn("{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

    Optional<Account> account = accountsManager.getByE164("+14152222222");

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(commands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidInCache() {
    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("Account3::" + uuid))).thenReturn("{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

    Optional<Account> account = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(commands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetByPniInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    when(commands.get(eq("AccountMap::" + pni))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid))).thenReturn("{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

    Optional<Account> account = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(commands).get(eq("AccountMap::" + pni));
    verify(commands).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(commands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetByUsernameInCache() {
    UUID uuid = UUID.randomUUID();
    String username = "test";

    when(commands.get(eq("UAccountMap::" + username))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid))).thenReturn("{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\", \"username\": \"test\"}");

    Optional<Account> account = accountsManager.getByUsername(username);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());
    assertEquals(Optional.of(username), account.get().getUsername());

    verify(commands).get(eq("UAccountMap::" + username));
    verify(commands).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(commands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByNumberNotInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(null);
    when(accounts.getByE164(eq("+14152222222"))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByE164("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).getByE164(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidNotInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).getByAccountIdentifier(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniNotInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(commands.get(eq("AccountMap::" + pni))).thenReturn(null);
    when(accounts.getByPhoneNumberIdentifier(pni)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("AccountMap::" + pni));
    verify(commands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByPhoneNumberIdentifier(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUsernameNotInCache() {
    UUID uuid = UUID.randomUUID();
    String username = "test";

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account.setUsername(username);

    when(commands.get(eq("UAccountMap::" + username))).thenReturn(null);
    when(accounts.getByUsername(username)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByUsername(username);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("UAccountMap::" + username));
    verify(commands).setex(eq("UAccountMap::" + username), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::" + account.getPhoneNumberIdentifier()), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByUsername(username);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByNumberBrokenCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(commands.get(eq("AccountMap::+14152222222"))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.getByE164(eq("+14152222222"))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByE164("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).getByE164(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidBrokenCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(commands.get(eq("Account3::" + uuid))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands, times(1)).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).getByAccountIdentifier(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniBrokenCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(commands.get(eq("AccountMap::" + pni))).thenThrow(new RedisException("OH NO"));
    when(accounts.getByPhoneNumberIdentifier(pni)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("AccountMap::" + pni));
    verify(commands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByPhoneNumberIdentifier(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUsernameBrokenCache() {
    UUID uuid = UUID.randomUUID();
    String username = "test";

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account.setUsername(username);

    when(commands.get(eq("UAccountMap::" + username))).thenThrow(new RedisException("OH NO"));
    when(accounts.getByUsername(username)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByUsername(username);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("UAccountMap::" + username));
    verify(commands).setex(eq("UAccountMap::" + username), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::" + account.getPhoneNumberIdentifier()), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByUsername(username);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testUpdate_optimisticLockingFailure() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.getByAccountIdentifier(uuid)).thenReturn(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16])));
    doThrow(ContestedOptimisticLockException.class)
        .doAnswer(ACCOUNT_UPDATE_ANSWER)
        .when(accounts).update(any());

    when(accounts.getByAccountIdentifier(uuid)).thenReturn(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16])));
    doThrow(ContestedOptimisticLockException.class)
        .doAnswer(ACCOUNT_UPDATE_ANSWER)
        .when(accounts).update(any());

    account = accountsManager.update(account, a -> a.setIdentityKey("identity-key"));

    assertEquals(1, account.getVersion());
    assertEquals("identity-key", account.getIdentityKey());

    verify(accounts, times(1)).getByAccountIdentifier(uuid);
    verify(accounts, times(2)).update(any());
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testUpdate_dynamoOptimisticLockingFailureDuringCreate() {
    UUID uuid = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.getByAccountIdentifier(uuid)).thenReturn(Optional.empty())
        .thenReturn(Optional.of(account));
    when(accounts.create(any())).thenThrow(ContestedOptimisticLockException.class);

    accountsManager.update(account, a -> {
    });

    verify(accounts, times(1)).update(account);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testUpdateDevice() {
    final UUID uuid = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    when(accounts.getByAccountIdentifier(uuid)).thenReturn(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16])));

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
  void testCreateFreshAccount() throws InterruptedException {
    when(accounts.create(any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true, null);
    accountsManager.create(e164, "password", null, attributes, new ArrayList<>());

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
    accountsManager.create(e164, "password", null, attributes, new ArrayList<>());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(e164));

    verify(accounts)
        .create(argThat(account -> e164.equals(account.getNumber()) && existingUuid.equals(account.getUuid())));

    verify(keys).delete(existingUuid);
    verify(keys).delete(phoneNumberIdentifiersByE164.get(e164));
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
    accountsManager.create(e164, "password", null, attributes, new ArrayList<>());

    verify(accounts).create(
        argThat(account -> e164.equals(account.getNumber()) && recentlyDeletedUuid.equals(account.getUuid())));
    verifyNoInteractions(keys);
    verifyNoInteractions(messagesManager);
    verifyNoInteractions(profilesManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithDiscoverability(final boolean discoverable) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, discoverable, null);
    final Account account = accountsManager.create("+18005550123", "password", null, attributes, new ArrayList<>());

    assertEquals(discoverable, account.isDiscoverableByPhoneNumber());

    if (!discoverable) {
      verify(directoryQueue).deleteAccount(account);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithStorageCapability(final boolean hasStorage) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true,
        new DeviceCapabilities(hasStorage, false, false, false, false, false, false, false, false));

    final Account account = accountsManager.create("+18005550123", "password", null, attributes, new ArrayList<>());

    assertEquals(hasStorage, account.isStorageSupported());
  }

  @ParameterizedTest
  @MethodSource
  void testUpdateDirectoryQueue(final boolean visibleBeforeUpdate, final boolean visibleAfterUpdate,
      final boolean expectRefresh) {
    final Account account = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    // this sets up the appropriate result for Account#shouldBeVisibleInDirectory
    final Device device = generateTestDevice(0);
    account.addDevice(device);
    account.setDiscoverableByPhoneNumber(visibleBeforeUpdate);

    final Account updatedAccount = accountsManager.update(account,
        a -> a.setDiscoverableByPhoneNumber(visibleAfterUpdate));

    verify(directoryQueue, times(expectRefresh ? 1 : 0)).refreshAccount(updatedAccount);
  }

  @SuppressWarnings("unused")
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
    final Account account = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final Device device = generateTestDevice(initialLastSeen);
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

  @Test
  void testChangePhoneNumber() throws InterruptedException, MismatchedDevicesException {
    doAnswer(invocation -> invocation.getArgument(2, BiFunction.class).apply(Optional.empty(), Optional.empty()))
        .when(deletedAccountsManager).lockAndPut(anyString(), anyString(), any());

    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, new ArrayList<>(), new byte[16]);
    account = accountsManager.changeNumber(account, targetNumber, null, null, null);

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    verify(directoryQueue).changePhoneNumber(argThat(a -> a.getUuid().equals(uuid)), eq(originalNumber), eq(targetNumber));
    verify(keys).delete(originalPni);
    verify(keys).delete(phoneNumberIdentifiersByE164.get(targetNumber));
  }

  @Test
  void testChangePhoneNumberSameNumber() throws InterruptedException, MismatchedDevicesException {
    final String number = "+14152222222";

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account = accountsManager.changeNumber(account, number, null, null, null);

    assertEquals(number, account.getNumber());
    verify(deletedAccountsManager, never()).lockAndPut(anyString(), anyString(), any());
    verify(directoryQueue, never()).changePhoneNumber(any(), any(), any());
    verify(keys, never()).delete(any());
  }

  @Test
  void testChangePhoneNumberExistingAccount() throws InterruptedException, MismatchedDevicesException {
    doAnswer(invocation -> invocation.getArgument(2, BiFunction.class).apply(Optional.empty(), Optional.empty()))
        .when(deletedAccountsManager).lockAndPut(anyString(), anyString(), any());

    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID existingAccountUuid = UUID.randomUUID();
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[16]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, new ArrayList<>(), new byte[16]);
    account = accountsManager.changeNumber(account, targetNumber, null, null, null);

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    verify(directoryQueue).changePhoneNumber(argThat(a -> a.getUuid().equals(uuid)), eq(originalNumber), eq(targetNumber));
    verify(directoryQueue).deleteAccount(existingAccount);
    verify(keys).delete(originalPni);
    verify(keys).delete(targetPni);
  }

  @Test
  void testChangePhoneNumberViaUpdate() {
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID uuid = UUID.randomUUID();

    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    assertThrows(AssertionError.class, () -> accountsManager.update(account, a -> a.setNumber(targetNumber, UUID.randomUUID())));
  }

  @Test
  void testSetUsername() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "test";
    assertDoesNotThrow(() -> accountsManager.setUsername(account, nickname, null));
    verify(accounts).setUsername(eq(account),  startsWith(nickname));
  }

  @Test
  void testReserveUsername() throws UsernameNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "beethoven";
    accountsManager.reserveUsername(account, nickname);
    verify(accounts).reserveUsername(eq(account),  startsWith(nickname), any());
  }

  @Test
  void testSetReservedUsername() throws UsernameNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String reserved = "sCoObY.1234";
    setReservationHash(account, reserved);
    when(accounts.usernameAvailable(eq(Optional.of(RESERVATION_TOKEN)), eq(reserved))).thenReturn(true);
    accountsManager.confirmReservedUsername(account, reserved, RESERVATION_TOKEN);
    verify(accounts).confirmUsername(eq(account), eq(reserved), eq(RESERVATION_TOKEN));
  }

  @Test
  void testSetReservedHashNameMismatch() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    setReservationHash(account, "pluto.1234");
    when(accounts.usernameAvailable(eq(Optional.of(RESERVATION_TOKEN)), eq("pluto.1234"))).thenReturn(true);
    assertThrows(UsernameReservationNotFoundException.class,
        () -> accountsManager.confirmReservedUsername(account, "goofy.1234", RESERVATION_TOKEN));
  }

  @Test
  void testSetReservedHashAciMismatch() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String reserved = "toto.1234";
    account.setReservedUsernameHash(Accounts.reservedUsernameHash(UUID.randomUUID(), reserved));
    when(accounts.usernameAvailable(eq(Optional.of(RESERVATION_TOKEN)), eq(reserved))).thenReturn(true);
    assertThrows(UsernameReservationNotFoundException.class,
        () -> accountsManager.confirmReservedUsername(account, reserved, RESERVATION_TOKEN));
  }

  @Test
  void testSetReservedLapsed() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String reserved = "porkchop.1234";
    // name was reserved, but the reservation lapsed and another account took it
    setReservationHash(account, reserved);
    when(accounts.usernameAvailable(eq(Optional.of(RESERVATION_TOKEN)), eq(reserved))).thenReturn(false);
    assertThrows(UsernameNotAvailableException.class, () -> accountsManager.confirmReservedUsername(account, reserved, RESERVATION_TOKEN));
    verify(accounts, never()).confirmUsername(any(), any(), any());
  }

  @Test
  void testSetReservedRetry() throws UsernameNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String username = "santaslittlehelper.1234";
    account.setUsername(username);

    // reserved username already set, should be treated as a replay
    accountsManager.confirmReservedUsername(account, username, RESERVATION_TOKEN);
    verifyNoInteractions(accounts);
  }

  @Test
  void testSetUsernameSameUsername() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "test";
    account.setUsername(nickname + ".123");

    // should be treated as a replayed request
    assertDoesNotThrow(() -> accountsManager.setUsername(account, nickname, null));
    verify(accounts, never()).setUsername(eq(account), any());
  }

  @Test
  void testSetUsernameReroll() throws UsernameNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "test";
    final String username = nickname + ".ZZZ";
    account.setUsername(username);

    // given the correct old username, should reroll discriminator even if the nick matches
    accountsManager.setUsername(account, nickname, username);
    verify(accounts).setUsername(eq(account), and(startsWith(nickname), not(eq(username))));
  }

  @Test
  void testReserveUsernameReroll() throws UsernameNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "clifford";
    final String username = nickname + ".ZZZ";
    account.setUsername(username);

    // given the correct old username, should reroll discriminator even if the nick matches
    accountsManager.reserveUsername(account, nickname);
    verify(accounts).reserveUsername(eq(account), and(startsWith(nickname), not(eq(username))), any());
  }

  @Test
  void testSetReservedUsernameWithNoReservation() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        new ArrayList<>(), new byte[16]);
    assertThrows(UsernameReservationNotFoundException.class,
        () -> accountsManager.confirmReservedUsername(account, "laika.1234", RESERVATION_TOKEN));
    verify(accounts, never()).confirmUsername(any(), any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testUsernameExpandDiscriminator(boolean reserve) throws UsernameNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "test";

    ArgumentMatcher<String> isWide = (String username) -> {
      String[] spl = username.split(Pattern.quote(UsernameGenerator.SEPARATOR));
      assertEquals(spl.length, 2);
      int discriminator = Integer.parseInt(spl[1]);
      // require a 7 digit discriminator
      return discriminator > 1_000_000;
    };
    when(accounts.usernameAvailable(any())).thenReturn(false);
    when(accounts.usernameAvailable(argThat(isWide))).thenReturn(true);

    if (reserve) {
      accountsManager.reserveUsername(account, nickname);
      verify(accounts).reserveUsername(eq(account), and(startsWith(nickname), argThat(isWide)), any());

    } else {
      accountsManager.setUsername(account, nickname, null);
      verify(accounts).setUsername(eq(account), and(startsWith(nickname), argThat(isWide)));
    }
  }

  @Test
  void testChangeUsername() throws UsernameNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "test";
    account.setUsername("old.123");
    accountsManager.setUsername(account, nickname, "old.123");
    verify(accounts).setUsername(eq(account),  startsWith(nickname));
  }

  @Test
  void testSetUsernameNotAvailable() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final String nickname = "unavailable";
    when(accounts.usernameAvailable(startsWith(nickname))).thenReturn(false);
    assertThrows(UsernameNotAvailableException.class, () -> accountsManager.setUsername(account, nickname, null));
    verify(accounts, never()).setUsername(any(), any());
    assertTrue(account.getUsername().isEmpty());
  }

  @Test
  void testSetUsernameReserved() {
    final String nickname = "reserved";
    when(prohibitedUsernames.isProhibited(eq(nickname), any())).thenReturn(true);

    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    assertThrows(UsernameNotAvailableException.class, () -> accountsManager.setUsername(account, nickname, null));
    assertTrue(account.getUsername().isEmpty());
  }

  @Test
  void testSetUsernameViaUpdate() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    assertThrows(AssertionError.class, () -> accountsManager.update(account, a -> a.setUsername("test")));
  }

  @Test
  void testSetUsernameDisabled() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    when(enrollmentManager.isEnrolled(account.getUuid(), AccountsManager.USERNAME_EXPERIMENT_NAME)).thenReturn(false);
    assertThrows(UsernameNotAvailableException.class, () -> accountsManager.setUsername(account, "n00bkiller", null));
  }

  private void setReservationHash(final Account account, final String reservedUsername) {
    account.setReservedUsernameHash(Accounts.reservedUsernameHash(account.getUuid(), reservedUsername));
  }

  private static Device generateTestDevice(final long lastSeen) {
    final Device device = new Device();
    device.setId(Device.MASTER_ID);
    device.setFetchesMessages(true);
    device.setSignedPreKey(new SignedPreKey(1, "key", "sig"));
    device.setLastSeen(lastSeen);

    return device;
  }
}
