/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
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
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AccountsManagerTest {
  private static final String BASE_64_URL_USERNAME_HASH_1 = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final String BASE_64_URL_USERNAME_HASH_2 = "NLUom-CHwtemcdvOTTXdmXmzRIV7F05leS8lwkVK_vc";
  private static final String BASE_64_URL_ENCRYPTED_USERNAME_1 = "md1votbj9r794DsqTNrBqA";
  private static final String BASE_64_URL_ENCRYPTED_USERNAME_2 = "9hrqVLy59bzgPse-S9NUsA";

  private static final byte[] USERNAME_HASH_1 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_1);
  private static final byte[] USERNAME_HASH_2 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_2);
  private static final byte[] ENCRYPTED_USERNAME_1 = Base64.getUrlDecoder().decode(BASE_64_URL_ENCRYPTED_USERNAME_1);
  private static final byte[] ENCRYPTED_USERNAME_2 = Base64.getUrlDecoder().decode(BASE_64_URL_ENCRYPTED_USERNAME_2);

  private Accounts accounts;
  private KeysManager keysManager;
  private MessagesManager messagesManager;
  private ProfilesManager profilesManager;
  private ClientPresenceManager clientPresenceManager;
  private ExperimentEnrollmentManager enrollmentManager;

  private Map<String, UUID> phoneNumberIdentifiersByE164;

  private RedisAdvancedClusterCommands<String, String> commands;
  private RedisAdvancedClusterAsyncCommands<String, String> asyncCommands;
  private AccountsManager accountsManager;

  private static final Answer<?> ACCOUNT_UPDATE_ANSWER = (answer) -> {
    // it is implicit in the update() contract is that a successful call will
    // result in an incremented version
    final Account updatedAccount = answer.getArgument(0, Account.class);
    updatedAccount.setVersion(updatedAccount.getVersion() + 1);
    return null;
  };

  private static final Answer<CompletableFuture<Void>> ACCOUNT_UPDATE_ASYNC_ANSWER = invocation -> {
    // it is implicit in the update() contract is that a successful call will
    // result in an incremented version
    final Account updatedAccount = invocation.getArgument(0, Account.class);
    updatedAccount.setVersion(updatedAccount.getVersion() + 1);

    return CompletableFuture.completedFuture(null);
  };

  @BeforeEach
  void setup() throws InterruptedException {
    accounts = mock(Accounts.class);
    keysManager = mock(KeysManager.class);
    messagesManager = mock(MessagesManager.class);
    profilesManager = mock(ProfilesManager.class);
    clientPresenceManager = mock(ClientPresenceManager.class);

    //noinspection unchecked
    commands = mock(RedisAdvancedClusterCommands.class);

    //noinspection unchecked
    asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class);
    when(asyncCommands.del(any())).thenReturn(MockRedisFuture.completedFuture(0L));
    when(asyncCommands.get(any())).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.updateAsync(any())).thenReturn(CompletableFuture.completedFuture(null));

    doAnswer((Answer<Void>) invocation -> {
      final Account account = invocation.getArgument(0, Account.class);
      final String number = invocation.getArgument(1, String.class);
      final UUID phoneNumberIdentifier = invocation.getArgument(2, UUID.class);

      account.setNumber(number, phoneNumberIdentifier);

      return null;
    }).when(accounts).changeNumber(any(), anyString(), any(), any());

    final SecureStorageClient storageClient = mock(SecureStorageClient.class);
    when(storageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureBackupClient backupClient = mock(SecureBackupClient.class);
    when(backupClient.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SecureValueRecovery2Client svr2Client = mock(SecureValueRecovery2Client.class);
    when(svr2Client.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

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
    when(accounts.usernameHashAvailable(any())).thenReturn(true);

    final AccountLockManager accountLockManager = mock(AccountLockManager.class);

    doAnswer(invocation -> {
      final Runnable task = invocation.getArgument(1);
      task.run();

      return null;
    }).when(accountLockManager).withLock(any(), any());

    when(keysManager.delete(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        RedisClusterHelper.builder()
            .stringCommands(commands)
            .stringAsyncCommands(asyncCommands)
            .build(),
        accountLockManager,
        keysManager,
        messagesManager,
        profilesManager,
        storageClient,
        backupClient,
        svr2Client,
        clientPresenceManager,
        enrollmentManager,
        mock(RegistrationRecoveryPasswordsManager.class),
        mock(Clock.class));
  }

  @Test
  void testGetByServiceIdentifier() {
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    when(commands.get(eq("AccountMap::" + pni))).thenReturn(aci.toString());
    when(commands.get(eq("Account3::" + aci))).thenReturn(
        "{\"number\": \"+14152222222\", \"pni\": \"" + pni + "\"}");

    assertTrue(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(aci)).isPresent());
    assertTrue(accountsManager.getByServiceIdentifier(new PniServiceIdentifier(pni)).isPresent());
    assertFalse(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(pni)).isPresent());
    assertFalse(accountsManager.getByServiceIdentifier(new PniServiceIdentifier(aci)).isPresent());
  }

  @Test
  void testGetByServiceIdentifierAsync() {
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    when(asyncCommands.get(eq("AccountMap::" + pni))).thenReturn(MockRedisFuture.completedFuture(aci.toString()));
    when(asyncCommands.get(eq("Account3::" + aci))).thenReturn(MockRedisFuture.completedFuture(
        "{\"number\": \"+14152222222\", \"pni\": \"" + pni + "\"}"));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByAccountIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(accounts.getByPhoneNumberIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    assertTrue(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(aci)).join().isPresent());
    assertTrue(accountsManager.getByServiceIdentifierAsync(new PniServiceIdentifier(pni)).join().isPresent());
    assertFalse(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(pni)).join().isPresent());
    assertFalse(accountsManager.getByServiceIdentifierAsync(new PniServiceIdentifier(aci)).join().isPresent());
  }

  @Test
  void testGetAccountByNumberInCache() {
    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid))).thenReturn(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

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
  void testGetAccountByNumberAsyncInCache() {
    UUID uuid = UUID.randomUUID();

    when(asyncCommands.get(eq("AccountMap::+14152222222")))
        .thenReturn(MockRedisFuture.completedFuture(uuid.toString()));

    when(asyncCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}"));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    Optional<Account> account = accountsManager.getByE164Async("+14152222222").join();

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(asyncCommands).get(eq("AccountMap::+14152222222"));
    verify(asyncCommands).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(asyncCommands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidInCache() {
    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("Account3::" + uuid))).thenReturn(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

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
  void testGetAccountByUuidInCacheAsync() {
    UUID uuid = UUID.randomUUID();

    when(asyncCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}"));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    Optional<Account> account = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(asyncCommands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(asyncCommands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByPniInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    when(commands.get(eq("AccountMap::" + pni))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid))).thenReturn(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

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
  void testGetAccountByPniInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    when(asyncCommands.get(eq("AccountMap::" + pni)))
        .thenReturn(MockRedisFuture.completedFuture(uuid.toString()));

    when(asyncCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}"));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    Optional<Account> account = accountsManager.getByPhoneNumberIdentifierAsync(pni).join();

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(asyncCommands).get(eq("AccountMap::" + pni));
    verify(asyncCommands).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(asyncCommands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByUsernameHashInCache() {
    UUID uuid = UUID.randomUUID();
    when(commands.get(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid))).thenReturn(
        String.format("{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\", \"usernameHash\": \"%s\"}",
            BASE_64_URL_USERNAME_HASH_1));

    Optional<Account> account = accountsManager.getByUsernameHash(USERNAME_HASH_1);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());
    assertArrayEquals(USERNAME_HASH_1, account.get().getUsernameHash().get());

    verify(commands).get(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1));
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
  void testGetAccountByNumberNotInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(asyncCommands.get(eq("AccountMap::+14152222222"))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByE164Async(eq("+14152222222")))
        .thenReturn(MockRedisFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByE164Async("+14152222222").join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("AccountMap::+14152222222"));
    verify(asyncCommands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByE164Async(eq("+14152222222"));
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
  void testGetAccountByUuidNotInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(asyncCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("Account3::" + uuid));
    verify(asyncCommands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByAccountIdentifierAsync(eq(uuid));
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
  void testGetAccountByPniNotInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(asyncCommands.get(eq("AccountMap::" + pni))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByPhoneNumberIdentifierAsync(pni))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifierAsync(pni).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("AccountMap::" + pni));
    verify(asyncCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByPhoneNumberIdentifierAsync(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUsernameHashNotInCache() {
    UUID uuid = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account.setUsernameHash(USERNAME_HASH_1);

    when(commands.get(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1))).thenReturn(null);
    when(accounts.getByUsernameHash(USERNAME_HASH_1)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByUsernameHash(USERNAME_HASH_1);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1));
    verify(commands).setex(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::" + account.getPhoneNumberIdentifier()), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByUsernameHash(USERNAME_HASH_1);
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
  void testGetAccountByNumberBrokenCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(asyncCommands.get(eq("AccountMap::+14152222222")))
        .thenReturn(MockRedisFuture.failedFuture(new RedisException("Connection lost!")));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByE164Async(eq("+14152222222"))).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByE164Async("+14152222222").join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("AccountMap::+14152222222"));
    verify(asyncCommands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByE164Async(eq("+14152222222"));
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
  void testGetAccountByUuidBrokenCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(asyncCommands.get(eq("Account3::" + uuid)))
        .thenReturn(MockRedisFuture.failedFuture(new RedisException("Connection lost!")));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("Account3::" + uuid));
    verify(asyncCommands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByAccountIdentifierAsync(eq(uuid));
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
  void testGetAccountByPniBrokenCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(asyncCommands.get(eq("AccountMap::" + pni)))
        .thenReturn(MockRedisFuture.failedFuture(new RedisException("OH NO")));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByPhoneNumberIdentifierAsync(pni))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifierAsync(pni).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("AccountMap::" + pni));
    verify(asyncCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByPhoneNumberIdentifierAsync(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUsernameBrokenCache() {
    UUID uuid = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account.setUsernameHash(USERNAME_HASH_1);

    when(commands.get(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1))).thenThrow(new RedisException("OH NO"));
    when(accounts.getByUsernameHash(USERNAME_HASH_1)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByUsernameHash(USERNAME_HASH_1);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1));
    verify(commands).setex(eq("UAccountMap::" + BASE_64_URL_USERNAME_HASH_1), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::" + account.getPhoneNumberIdentifier()), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("AccountMap::+14152222222"), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByUsernameHash(USERNAME_HASH_1);
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

    final IdentityKey identityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    account = accountsManager.update(account, a -> a.setIdentityKey(identityKey));

    assertEquals(1, account.getVersion());
    assertEquals(identityKey, account.getIdentityKey(IdentityType.ACI));

    verify(accounts, times(1)).getByAccountIdentifier(uuid);
    verify(accounts, times(2)).update(any());
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testUpdateAsync_optimisticLockingFailure() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]);

    when(asyncCommands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.getByAccountIdentifierAsync(uuid)).thenReturn(CompletableFuture.completedFuture(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[16]))));

    when(accounts.updateAsync(any()))
        .thenReturn(CompletableFuture.failedFuture(new ContestedOptimisticLockException()))
        .thenAnswer(ACCOUNT_UPDATE_ASYNC_ANSWER);

    final IdentityKey identityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    account = accountsManager.updateAsync(account, a -> a.setIdentityKey(identityKey)).join();

    assertEquals(1, account.getVersion());
    assertEquals(identityKey, account.getIdentityKey(IdentityType.ACI));

    verify(accounts, times(1)).getByAccountIdentifierAsync(uuid);
    verify(accounts, times(2)).updateAsync(any());
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
    enabledDevice.setSignedPreKey(KeysHelper.signedECPreKey(1, Curve.generateKeyPair()));
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
  void testUpdateDeviceAsync() {
    final UUID uuid = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    when(accounts.getByAccountIdentifierAsync(uuid)).thenReturn(CompletableFuture.completedFuture(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[16]))));

    assertTrue(account.getDevices().isEmpty());

    Device enabledDevice = new Device();
    enabledDevice.setFetchesMessages(true);
    enabledDevice.setSignedPreKey(KeysHelper.signedECPreKey(1, Curve.generateKeyPair()));
    enabledDevice.setLastSeen(System.currentTimeMillis());
    final long deviceId = account.getNextDeviceId();
    enabledDevice.setId(deviceId);
    account.addDevice(enabledDevice);

    @SuppressWarnings("unchecked") Consumer<Device> deviceUpdater = mock(Consumer.class);
    @SuppressWarnings("unchecked") Consumer<Device> unknownDeviceUpdater = mock(Consumer.class);

    account = accountsManager.updateDeviceAsync(account, deviceId, deviceUpdater).join();
    account = accountsManager.updateDeviceAsync(account, deviceId, d -> d.setName("deviceName")).join();

    assertEquals("deviceName", account.getDevice(deviceId).orElseThrow().getName());

    verify(deviceUpdater, times(1)).accept(any(Device.class));

    accountsManager.updateDeviceAsync(account, account.getNextDeviceId(), unknownDeviceUpdater).join();

    verify(unknownDeviceUpdater, never()).accept(any(Device.class));
  }

  @Test
  void testCreateFreshAccount() throws InterruptedException {
    when(accounts.create(any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true, null);
    accountsManager.create(e164, "password", null, attributes, new ArrayList<>());

    verify(accounts).create(argThat(account -> e164.equals(account.getNumber())));
    verifyNoInteractions(keysManager);
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

    verify(keysManager).delete(existingUuid);
    verify(keysManager).delete(phoneNumberIdentifiersByE164.get(e164));
    verify(messagesManager).clear(existingUuid);
    verify(profilesManager).deleteAll(existingUuid);
    verify(clientPresenceManager).disconnectAllPresencesForUuid(existingUuid);
  }

  @Test
  void testCreateAccountRecentlyDeleted() throws InterruptedException {
    final UUID recentlyDeletedUuid = UUID.randomUUID();

    when(accounts.findRecentlyDeletedAccountIdentifier(anyString())).thenReturn(Optional.of(recentlyDeletedUuid));
    when(accounts.create(any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true, null);
    accountsManager.create(e164, "password", null, attributes, new ArrayList<>());

    verify(accounts).create(
        argThat(account -> e164.equals(account.getNumber()) && recentlyDeletedUuid.equals(account.getUuid())));
    verifyNoInteractions(keysManager);
    verifyNoInteractions(messagesManager);
    verifyNoInteractions(profilesManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithDiscoverability(final boolean discoverable) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, discoverable, null);
    final Account account = accountsManager.create("+18005550123", "password", null, attributes, new ArrayList<>());

    assertEquals(discoverable, account.isDiscoverableByPhoneNumber());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithStorageCapability(final boolean hasStorage) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 0, null, null, true,
        new DeviceCapabilities(hasStorage, false, false, false));

    final Account account = accountsManager.create("+18005550123", "password", null, attributes, new ArrayList<>());

    assertEquals(hasStorage, account.isStorageSupported());
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
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, new ArrayList<>(), new byte[16]);
    account = accountsManager.changeNumber(account, targetNumber, null, null, null, null);

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    verify(keysManager).delete(originalPni);
    verify(keysManager).delete(phoneNumberIdentifiersByE164.get(targetNumber));
  }

  @Test
  void testChangePhoneNumberSameNumber() throws InterruptedException, MismatchedDevicesException {
    final String number = "+14152222222";

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account = accountsManager.changeNumber(account, number, null, null, null, null);

    assertEquals(number, account.getNumber());
    verify(keysManager, never()).delete(any());
  }

  @Test
  void testChangePhoneNumberSameNumberWithPniData() {
    final String number = "+14152222222";

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    assertThrows(IllegalArgumentException.class,
        () -> accountsManager.changeNumber(
            account, number, new IdentityKey(Curve.generateKeyPair().getPublicKey()), Map.of(1L, KeysHelper.signedECPreKey(1, pniIdentityKeyPair)), null, Map.of(1L, 101)),
        "AccountsManager should not allow use of changeNumber with new PNI keys but without changing number");

    verify(accounts, never()).update(any());
    verifyNoInteractions(keysManager);
  }

  @Test
  void testChangePhoneNumberExistingAccount() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID existingAccountUuid = UUID.randomUUID();
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[16]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, new ArrayList<>(), new byte[16]);
    account = accountsManager.changeNumber(account, targetNumber, null, null, null, null);

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));
    final UUID newPni = phoneNumberIdentifiersByE164.get(targetNumber);

    verify(keysManager).delete(existingAccountUuid);
    verify(keysManager).delete(originalPni);
    verify(keysManager, atLeastOnce()).delete(targetPni);
    verify(keysManager).delete(newPni);
    verify(keysManager).storeEcSignedPreKeys(eq(newPni), any());
    verifyNoMoreInteractions(keysManager);
  }

  @Test
  void testChangePhoneNumberWithPqKeysExistingAccount() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID existingAccountUuid = UUID.randomUUID();
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Long, ECSignedPreKey> newSignedKeys = Map.of(
        1L, KeysHelper.signedECPreKey(1, identityKeyPair),
        2L, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Long, KEMSignedPreKey> newSignedPqKeys = Map.of(
        1L, KeysHelper.signedKEMPreKey(3, identityKeyPair),
        2L, KeysHelper.signedKEMPreKey(4, identityKeyPair));
    final Map<Long, Integer> newRegistrationIds = Map.of(1L, 201, 2L, 202);

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[16]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));
    when(keysManager.getPqEnabledDevices(uuid)).thenReturn(CompletableFuture.completedFuture(List.of(1L)));

    final List<Device> devices = List.of(DevicesHelper.createDevice(1L, 0L, 101), DevicesHelper.createDevice(2L, 0L, 102));
    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, devices, new byte[16]);
    final Account updatedAccount = accountsManager.changeNumber(
        account, targetNumber, new IdentityKey(Curve.generateKeyPair().getPublicKey()), newSignedKeys, newSignedPqKeys, newRegistrationIds);

    assertEquals(targetNumber, updatedAccount.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    final UUID newPni = phoneNumberIdentifiersByE164.get(targetNumber);
    verify(keysManager).delete(existingAccountUuid);
    verify(keysManager, atLeastOnce()).delete(targetPni);
    verify(keysManager).delete(newPni);
    verify(keysManager).delete(originalPni);
    verify(keysManager).getPqEnabledDevices(uuid);
    verify(keysManager).storeEcSignedPreKeys(newPni, newSignedKeys);
    verify(keysManager).storePqLastResort(eq(newPni), eq(Map.of(1L, newSignedPqKeys.get(1L))));
    verifyNoMoreInteractions(keysManager);
  }


  @Test
  void testChangePhoneNumberWithMismatchedPqKeys() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID existingAccountUuid = UUID.randomUUID();
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Long, ECSignedPreKey> newSignedKeys = Map.of(
        1L, KeysHelper.signedECPreKey(1, identityKeyPair),
        2L, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Long, KEMSignedPreKey> newSignedPqKeys = Map.of(
        1L, KeysHelper.signedKEMPreKey(3, identityKeyPair));
    final Map<Long, Integer> newRegistrationIds = Map.of(1L, 201, 2L, 202);

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[16]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));
    when(keysManager.getPqEnabledDevices(uuid)).thenReturn(CompletableFuture.completedFuture(List.of(1L)));

    final List<Device> devices = List.of(DevicesHelper.createDevice(1L, 0L, 101), DevicesHelper.createDevice(2L, 0L, 102));
    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, devices, new byte[16]);
    assertThrows(MismatchedDevicesException.class,
        () -> accountsManager.changeNumber(
            account, targetNumber, new IdentityKey(Curve.generateKeyPair().getPublicKey()), newSignedKeys, newSignedPqKeys, newRegistrationIds));

    verifyNoInteractions(accounts);
    verifyNoInteractions(keysManager);
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
  void testPniUpdate() throws MismatchedDevicesException {
    final String number = "+14152222222";

    List<Device> devices = List.of(DevicesHelper.createDevice(1L, 0L, 101), DevicesHelper.createDevice(2L, 0L, 102));
    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[16]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    Map<Long, ECSignedPreKey> newSignedKeys = Map.of(
        1L, KeysHelper.signedECPreKey(1, identityKeyPair),
        2L, KeysHelper.signedECPreKey(2, identityKeyPair));
    Map<Long, Integer> newRegistrationIds = Map.of(1L, 201, 2L, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();
    Map<Long, ECSignedPreKey> oldSignedPreKeys = account.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI)));

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    when(keysManager.getPqEnabledDevices(any())).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    when(keysManager.storeEcSignedPreKeys(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Account updatedAccount = accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, null, newRegistrationIds);

    // non-PNI stuff should not change
    assertEquals(oldUuid, updatedAccount.getUuid());
    assertEquals(number, updatedAccount.getNumber());
    assertEquals(oldPni, updatedAccount.getPhoneNumberIdentifier());
    assertNull(updatedAccount.getIdentityKey(IdentityType.ACI));
    assertEquals(oldSignedPreKeys, updatedAccount.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI))));
    assertEquals(Map.of(1L, 101, 2L, 102),
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::getRegistrationId)));

    // PNI stuff should
    assertEquals(pniIdentityKey, updatedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(newSignedKeys,
        updatedAccount.getDevices().stream()
            .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.PNI))));
    assertEquals(newRegistrationIds,
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, d -> d.getPhoneNumberIdentityRegistrationId().getAsInt())));

    verify(accounts).update(any());

    verify(keysManager).delete(oldPni);
  }

  @Test
  void testPniPqUpdate() throws MismatchedDevicesException {
    final String number = "+14152222222";

    List<Device> devices = List.of(DevicesHelper.createDevice(1L, 0L, 101), DevicesHelper.createDevice(2L, 0L, 102));
    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[16]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Long, ECSignedPreKey> newSignedKeys = Map.of(
        1L, KeysHelper.signedECPreKey(1, identityKeyPair),
        2L, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Long, KEMSignedPreKey> newSignedPqKeys = Map.of(
        1L, KeysHelper.signedKEMPreKey(3, identityKeyPair),
        2L, KeysHelper.signedKEMPreKey(4, identityKeyPair));
    Map<Long, Integer> newRegistrationIds = Map.of(1L, 201, 2L, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();

    when(keysManager.getPqEnabledDevices(oldPni)).thenReturn(CompletableFuture.completedFuture(List.of(1L)));
    when(keysManager.storeEcSignedPreKeys(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.storePqLastResort(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    Map<Long, ECSignedPreKey> oldSignedPreKeys = account.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI)));

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    final Account updatedAccount =
        accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, newSignedPqKeys, newRegistrationIds);

    // non-PNI-keys stuff should not change
    assertEquals(oldUuid, updatedAccount.getUuid());
    assertEquals(number, updatedAccount.getNumber());
    assertEquals(oldPni, updatedAccount.getPhoneNumberIdentifier());
    assertNull(updatedAccount.getIdentityKey(IdentityType.ACI));
    assertEquals(oldSignedPreKeys, updatedAccount.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI))));
    assertEquals(Map.of(1L, 101, 2L, 102),
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::getRegistrationId)));

    // PNI keys should
    assertEquals(pniIdentityKey, updatedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(newSignedKeys,
        updatedAccount.getDevices().stream()
            .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.PNI))));
    assertEquals(newRegistrationIds,
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, d -> d.getPhoneNumberIdentityRegistrationId().getAsInt())));

    verify(accounts).update(any());

    verify(keysManager).delete(oldPni);
    verify(keysManager).storeEcSignedPreKeys(oldPni, newSignedKeys);

    // only the pq key for the already-pq-enabled device should be saved
    verify(keysManager).storePqLastResort(eq(oldPni), eq(Map.of(1L, newSignedPqKeys.get(1L))));
  }

  @Test
  void testPniNonPqToPqUpdate() throws MismatchedDevicesException {
    final String number = "+14152222222";

    List<Device> devices = List.of(DevicesHelper.createDevice(1L, 0L, 101), DevicesHelper.createDevice(2L, 0L, 102));
    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[16]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Long, ECSignedPreKey> newSignedKeys = Map.of(
        1L, KeysHelper.signedECPreKey(1, identityKeyPair),
        2L, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Long, KEMSignedPreKey> newSignedPqKeys = Map.of(
        1L, KeysHelper.signedKEMPreKey(3, identityKeyPair),
        2L, KeysHelper.signedKEMPreKey(4, identityKeyPair));
    Map<Long, Integer> newRegistrationIds = Map.of(1L, 201, 2L, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();

    when(keysManager.getPqEnabledDevices(oldPni)).thenReturn(CompletableFuture.completedFuture(List.of()));
    when(keysManager.storeEcSignedPreKeys(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.storePqLastResort(any(), any())).thenAnswer(
        invocation -> {
          assertFalse(invocation.getArgument(1, Map.class).isEmpty());
          return CompletableFuture.completedFuture(null);
        });

    Map<Long, ECSignedPreKey> oldSignedPreKeys = account.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI)));

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    final Account updatedAccount =
        accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, newSignedPqKeys, newRegistrationIds);

    // non-PNI-keys stuff should not change
    assertEquals(oldUuid, updatedAccount.getUuid());
    assertEquals(number, updatedAccount.getNumber());
    assertEquals(oldPni, updatedAccount.getPhoneNumberIdentifier());
    assertNull(updatedAccount.getIdentityKey(IdentityType.ACI));
    assertEquals(oldSignedPreKeys, updatedAccount.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI))));
    assertEquals(Map.of(1L, 101, 2L, 102),
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::getRegistrationId)));

    // PNI keys should
    assertEquals(pniIdentityKey, updatedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(newSignedKeys,
        updatedAccount.getDevices().stream()
            .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.PNI))));
    assertEquals(newRegistrationIds,
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, d -> d.getPhoneNumberIdentityRegistrationId().getAsInt())));

    verify(accounts).update(any());

    verify(keysManager).delete(oldPni);
    verify(keysManager).storeEcSignedPreKeys(oldPni, newSignedKeys);

    // no pq-enabled devices -> no pq last resort keys should be stored
    verify(keysManager, never()).storePqLastResort(any(), any());
  }

  @Test
  void testPniUpdate_incompleteKeys() {
    final String number = "+14152222222";

    List<Device> devices = List.of(DevicesHelper.createDevice(1L, 0L, 101), DevicesHelper.createDevice(2L, 0L, 102));
    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[16]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Long, ECSignedPreKey> newSignedKeys = Map.of(
        2L, KeysHelper.signedECPreKey(1, identityKeyPair),
        3L, KeysHelper.signedECPreKey(2, identityKeyPair));
    Map<Long, Integer> newRegistrationIds = Map.of(1L, 201, 2L, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();

    Map<Long, ECSignedPreKey> oldSignedPreKeys = account.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI)));

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    assertThrows(MismatchedDevicesException.class,
        () -> accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, null, newRegistrationIds));

    verifyNoInteractions(accounts);
    verifyNoInteractions(keysManager);
  }

  @Test
  void testPniPqUpdate_incompleteKeys() {
    final String number = "+14152222222";

    List<Device> devices = List.of(DevicesHelper.createDevice(1L, 0L, 101), DevicesHelper.createDevice(2L, 0L, 102));
    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[16]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Long, ECSignedPreKey> newSignedKeys = Map.of(
        1L, KeysHelper.signedECPreKey(1, identityKeyPair),
        2L, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Long, KEMSignedPreKey> newSignedPqKeys = Map.of(
        1L, KeysHelper.signedKEMPreKey(3, identityKeyPair));
    Map<Long, Integer> newRegistrationIds = Map.of(1L, 201, 2L, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();

    Map<Long, ECSignedPreKey> oldSignedPreKeys = account.getDevices().stream()
        .collect(Collectors.toMap(Device::getId, d -> d.getSignedPreKey(IdentityType.ACI)));

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    assertThrows(MismatchedDevicesException.class,
        () -> accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, newSignedPqKeys, newRegistrationIds));

    verifyNoInteractions(accounts);
    verifyNoInteractions(keysManager);
  }

  @Test
  void testReserveUsernameHash() throws UsernameHashNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    final List<byte[]> usernameHashes = List.of(new byte[32], new byte[32]);
    when(accounts.usernameHashAvailable(any())).thenReturn(true);
    accountsManager.reserveUsernameHash(account, usernameHashes);
    verify(accounts).reserveUsernameHash(eq(account), eq(new byte[32]), eq(Duration.ofMinutes(5)));
  }

  @Test
  void testReserveUsernameHashNotAvailable() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    when(accounts.usernameHashAvailable(any())).thenReturn(false);

    assertThrows(UsernameHashNotAvailableException.class, () -> accountsManager.reserveUsernameHash(account, List.of(
        USERNAME_HASH_1, USERNAME_HASH_2)));
  }

  @Test
  void testReserveUsernameDisabled() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    when(enrollmentManager.isEnrolled(account.getUuid(), AccountsManager.USERNAME_EXPERIMENT_NAME)).thenReturn(false);
    assertThrows(UsernameHashNotAvailableException.class, () -> accountsManager.reserveUsernameHash(account, List.of(
        USERNAME_HASH_1)));
  }

  @Test
  void testConfirmReservedUsernameHash() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    setReservationHash(account, USERNAME_HASH_1);
    when(accounts.usernameHashAvailable(eq(Optional.of(account.getUuid())), eq(USERNAME_HASH_1))).thenReturn(true);
    accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    verify(accounts).confirmUsernameHash(eq(account), eq(USERNAME_HASH_1), eq(ENCRYPTED_USERNAME_1));
  }

  @Test
  void testConfirmReservedHashNameMismatch() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    setReservationHash(account, USERNAME_HASH_1);
    when(accounts.usernameHashAvailable(eq(Optional.of(account.getUuid())), eq(USERNAME_HASH_1))).thenReturn(true);
    assertThrows(UsernameReservationNotFoundException.class,
        () -> accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_2, ENCRYPTED_USERNAME_2));
  }

  @Test
  void testConfirmReservedLapsed() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    // hash was reserved, but the reservation lapsed and another account took it
    setReservationHash(account, USERNAME_HASH_1);
    when(accounts.usernameHashAvailable(eq(Optional.of(account.getUuid())), eq(USERNAME_HASH_1))).thenReturn(false);
    assertThrows(UsernameHashNotAvailableException.class, () -> accountsManager.confirmReservedUsernameHash(account,
            USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    verify(accounts, never()).confirmUsernameHash(any(), any(), any());
  }

  @Test
  void testConfirmReservedRetry() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account.setUsernameHash(USERNAME_HASH_1);

    // reserved username already set, should be treated as a replay
    accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    verifyNoInteractions(accounts);
  }

  @Test
  void testConfirmReservedUsernameHashWithNoReservation() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        new ArrayList<>(), new byte[16]);
    assertThrows(UsernameReservationNotFoundException.class,
        () -> accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    verify(accounts, never()).confirmUsernameHash(any(), any(), any());
  }

  @Test
  void testClearUsernameHash() {
    Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);
    account.setUsernameHash(USERNAME_HASH_1);
    accountsManager.clearUsernameHash(account);
    verify(accounts).clearUsernameHash(eq(account));
  }

  @Test
  void testSetUsernameViaUpdate() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[16]);

    assertThrows(AssertionError.class, () -> accountsManager.update(account, a -> a.setUsernameHash(USERNAME_HASH_1)));
  }

  @Test
  void testJsonRoundTripSerialization() throws Exception {
    String originalJson;
    try (InputStream inputStream = getClass().getResourceAsStream(
        "AccountsManagerTest-testJsonRoundTripSerialization.json")) {
      Objects.requireNonNull(inputStream);
      originalJson = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    final Account originalAccount = AccountsManager.parseAccountJson(originalJson,
        UUID.fromString("111111-1111-1111-1111-111111111111")).orElseThrow();

    final String serialized = AccountsManager.writeRedisAccountJson(originalAccount);
    final Account parsedAccount = AccountsManager.parseAccountJson(serialized, originalAccount.getUuid()).orElseThrow();

    assertEquals(originalAccount.getUuid(), parsedAccount.getUuid());
    assertEquals(originalAccount.getPhoneNumberIdentifier(), parsedAccount.getPhoneNumberIdentifier());
    assertEquals(originalAccount.getIdentityKey(IdentityType.ACI), parsedAccount.getIdentityKey(IdentityType.ACI));
    assertEquals(originalAccount.getIdentityKey(IdentityType.PNI), parsedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(originalAccount.getNumber(), parsedAccount.getNumber());
    assertArrayEquals(originalAccount.getUnidentifiedAccessKey().orElseThrow(),
        parsedAccount.getUnidentifiedAccessKey().orElseThrow());
    assertEquals(originalAccount.isDiscoverableByPhoneNumber(), parsedAccount.isDiscoverableByPhoneNumber());
    assertEquals(originalAccount.isUnrestrictedUnidentifiedAccess(), parsedAccount.isUnrestrictedUnidentifiedAccess());

    assertEquals(originalAccount.getDevices().size(), parsedAccount.getDevices().size());

    final Device originalDevice = originalAccount.getMasterDevice().orElseThrow();
    final Device parsedDevice = parsedAccount.getMasterDevice().orElseThrow();

    assertEquals(originalDevice.getId(), parsedDevice.getId());
    assertEquals(originalDevice.getSignedPreKey(IdentityType.ACI), parsedDevice.getSignedPreKey(IdentityType.ACI));
    assertEquals(originalDevice.getSignedPreKey(IdentityType.PNI), parsedDevice.getSignedPreKey(IdentityType.PNI));
    assertEquals(originalDevice.getRegistrationId(), parsedDevice.getRegistrationId());
    assertEquals(originalDevice.getPhoneNumberIdentityRegistrationId(),
        parsedDevice.getPhoneNumberIdentityRegistrationId());
    assertEquals(originalDevice.getCapabilities(), parsedDevice.getCapabilities());
    assertEquals(originalDevice.getFetchesMessages(), parsedDevice.getFetchesMessages());
  }

  private void setReservationHash(final Account account, final byte[] reservedUsernameHash) {
    account.setReservedUsernameHash(reservedUsernameHash);
  }

  private static Device generateTestDevice(final long lastSeen) {
    final Device device = new Device();
    device.setId(Device.MASTER_ID);
    device.setFetchesMessages(true);
    device.setSignedPreKey(KeysHelper.signedECPreKey(1, Curve.generateKeyPair()));
    device.setLastSeen(lastSeen);

    return device;
  }
}
