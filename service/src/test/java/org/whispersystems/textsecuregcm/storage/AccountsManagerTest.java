/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
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

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryException;
import org.whispersystems.textsecuregcm.storage.AccountsManager.UsernameReservation;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

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
  private ClientPublicKeysManager clientPublicKeysManager;

  private Map<String, UUID> phoneNumberIdentifiersByE164;

  private RedisAdvancedClusterCommands<String, String> commands;
  private RedisAdvancedClusterAsyncCommands<String, String> asyncCommands;
  private TestClock clock;
  private AccountsManager accountsManager;
  private SecureValueRecovery2Client svr2Client;
  private DynamicConfiguration dynamicConfiguration;

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
    clientPublicKeysManager = mock(ClientPublicKeysManager.class);
    dynamicConfiguration = mock(DynamicConfiguration.class);

    final Executor clientPresenceExecutor = mock(Executor.class);

    doAnswer(invocation -> {
      final Runnable runnable = invocation.getArgument(0);
      runnable.run();

      return null;
    }).when(clientPresenceExecutor).execute(any());

    //noinspection unchecked
    commands = mock(RedisAdvancedClusterCommands.class);

    //noinspection unchecked
    asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class);
    when(asyncCommands.del(any(String[].class))).thenReturn(MockRedisFuture.completedFuture(0L));
    when(asyncCommands.get(any())).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.updateAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(accounts.updateTransactionallyAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(accounts.delete(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    doAnswer((Answer<Void>) invocation -> {
      final Account account = invocation.getArgument(0, Account.class);
      final String number = invocation.getArgument(1, String.class);
      final UUID phoneNumberIdentifier = invocation.getArgument(2, UUID.class);

      account.setNumber(number, phoneNumberIdentifier);

      return null;
    }).when(accounts).changeNumber(any(), anyString(), any(), any(), any());

    final SecureStorageClient storageClient = mock(SecureStorageClient.class);
    when(storageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

    svr2Client = mock(SecureValueRecovery2Client.class);
    when(svr2Client.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    phoneNumberIdentifiersByE164 = new HashMap<>();

    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString())).thenAnswer((Answer<UUID>) invocation -> {
      final String number = invocation.getArgument(0, String.class);
      return phoneNumberIdentifiersByE164.computeIfAbsent(number, n -> UUID.randomUUID());
    });

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getSvrStatusCodesToIgnoreForAccountDeletion()).thenReturn(Collections.emptyList());

    final AccountLockManager accountLockManager = mock(AccountLockManager.class);

    doAnswer(invocation -> {
      final Runnable task = invocation.getArgument(1);
      task.run();

      return null;
    }).when(accountLockManager).withLock(any(), any(), any());

    when(accountLockManager.withLockAsync(any(), any(), any())).thenAnswer(invocation -> {
      final Supplier<CompletableFuture<?>> taskSupplier = invocation.getArgument(1);
      return taskSupplier.get();
    });

    final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        mock(RegistrationRecoveryPasswordsManager.class);

    when(registrationRecoveryPasswordsManager.removeForNumber(anyString())).thenReturn(CompletableFuture.completedFuture(null));

    when(keysManager.deleteSingleUsePreKeys(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(profilesManager.deleteAll(any())).thenReturn(CompletableFuture.completedFuture(null));

    clock = TestClock.now();


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
        svr2Client,
        clientPresenceManager,
        registrationRecoveryPasswordsManager,
        clientPublicKeysManager,
        mock(Executor.class),
        clientPresenceExecutor,
        clock,
        dynamicConfigurationManager);
  }

  @ParameterizedTest
  @MethodSource
  void testDeleteWithSvrErrorStatusCodes(final String statusCode, final boolean expectError) throws InterruptedException {
    when(svr2Client.deleteBackups(any())).thenReturn(
        CompletableFuture.failedFuture(new SecureValueRecoveryException("Failed to delete backup", statusCode)));
    when(dynamicConfiguration.getSvrStatusCodesToIgnoreForAccountDeletion()).thenReturn(List.of("500"));

    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, true, null);

    final Account createdAccount = createAccount("+18005550123", attributes);

    if (expectError) {
      assertThrows(CompletionException.class, () -> accountsManager.delete(createdAccount, AccountsManager.DeletionReason.USER_REQUEST).toCompletableFuture().join());
    } else {
      assertDoesNotThrow(() -> accountsManager.delete(createdAccount, AccountsManager.DeletionReason.USER_REQUEST).toCompletableFuture().join());
    }
  }

  private static Stream<Arguments> testDeleteWithSvrErrorStatusCodes() {
    return Stream.of(
        Arguments.of("500", false),
        Arguments.of("429", true)
    );
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
  void testGetAccountByUuidNotInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
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
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("Account3::" + uuid));
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

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(commands.get(eq("AccountMap::" + pni))).thenReturn(null);
    when(accounts.getByPhoneNumberIdentifier(pni)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("AccountMap::" + pni));
    verify(commands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByPhoneNumberIdentifier(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniNotInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncCommands.get(eq("AccountMap::" + pni))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByPhoneNumberIdentifierAsync(pni))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifierAsync(pni).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("AccountMap::" + pni));
    verify(asyncCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByPhoneNumberIdentifierAsync(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUsernameHash() {
    UUID uuid = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(),
        new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account.setUsernameHash(USERNAME_HASH_1);
    when(accounts.getByUsernameHash(USERNAME_HASH_1))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    Optional<Account> retrieved = accountsManager.getByUsernameHash(USERNAME_HASH_1).join();
    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);
    verify(accounts).getByUsernameHash(USERNAME_HASH_1);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidBrokenCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(commands.get(eq("Account3::" + uuid))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
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
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncCommands.get(eq("Account3::" + uuid)))
        .thenReturn(MockRedisFuture.failedFuture(new RedisException("Connection lost!")));

    when(asyncCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncCommands).get(eq("Account3::" + uuid));
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

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(commands.get(eq("AccountMap::" + pni))).thenThrow(new RedisException("OH NO"));
    when(accounts.getByPhoneNumberIdentifier(pni)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands).get(eq("AccountMap::" + pni));
    verify(commands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(commands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts).getByPhoneNumberIdentifier(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniBrokenCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

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
    verify(asyncCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncCommands);

    verify(accounts).getByPhoneNumberIdentifierAsync(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testUpdate_optimisticLockingFailure() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.getByAccountIdentifier(uuid)).thenReturn(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])));
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
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncCommands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.getByAccountIdentifierAsync(uuid)).thenReturn(CompletableFuture.completedFuture(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]))));

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
  void testUpdate_dynamoOptimisticLockingFailureDuringCreate() throws AccountAlreadyExistsException {
    UUID uuid = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.getByAccountIdentifier(uuid)).thenReturn(Optional.empty())
        .thenReturn(Optional.of(account));
    when(accounts.create(any(), any())).thenThrow(ContestedOptimisticLockException.class);

    accountsManager.update(account, a -> {
    });

    verify(accounts, times(1)).update(account);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testUpdateDevice() {
    final UUID uuid = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(accounts.getByAccountIdentifier(uuid)).thenReturn(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])));

    assertTrue(account.getDevices().isEmpty());

    Device enabledDevice = new Device();
    enabledDevice.setFetchesMessages(true);
    enabledDevice.setLastSeen(System.currentTimeMillis());
    final byte deviceId = account.getNextDeviceId();
    enabledDevice.setId(deviceId);
    account.addDevice(enabledDevice);

    @SuppressWarnings("unchecked") Consumer<Device> deviceUpdater = mock(Consumer.class);
    @SuppressWarnings("unchecked") Consumer<Device> unknownDeviceUpdater = mock(Consumer.class);

    account = accountsManager.updateDevice(account, deviceId, deviceUpdater);
    account = accountsManager.updateDevice(account, deviceId, d -> d.setName("deviceName".getBytes(StandardCharsets.UTF_8)));

    assertArrayEquals("deviceName".getBytes(StandardCharsets.UTF_8), account.getDevice(deviceId).orElseThrow().getName());

    verify(deviceUpdater, times(1)).accept(any(Device.class));

    accountsManager.updateDevice(account, account.getNextDeviceId(), unknownDeviceUpdater);

    verify(unknownDeviceUpdater, never()).accept(any(Device.class));
  }

  @Test
  void testUpdateDeviceAsync() {
    final UUID uuid = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(accounts.getByAccountIdentifierAsync(uuid)).thenReturn(CompletableFuture.completedFuture(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]))));

    assertTrue(account.getDevices().isEmpty());

    Device enabledDevice = new Device();
    enabledDevice.setFetchesMessages(true);
    enabledDevice.setLastSeen(System.currentTimeMillis());
    final byte deviceId = account.getNextDeviceId();
    enabledDevice.setId(deviceId);
    account.addDevice(enabledDevice);

    @SuppressWarnings("unchecked") Consumer<Device> deviceUpdater = mock(Consumer.class);
    @SuppressWarnings("unchecked") Consumer<Device> unknownDeviceUpdater = mock(Consumer.class);

    account = accountsManager.updateDeviceAsync(account, deviceId, deviceUpdater).join();
    account = accountsManager.updateDeviceAsync(account, deviceId, d -> d.setName("deviceName".getBytes(StandardCharsets.UTF_8))).join();

    assertArrayEquals("deviceName".getBytes(StandardCharsets.UTF_8), account.getDevice(deviceId).orElseThrow().getName());

    verify(deviceUpdater, times(1)).accept(any(Device.class));

    accountsManager.updateDeviceAsync(account, account.getNextDeviceId(), unknownDeviceUpdater).join();

    verify(unknownDeviceUpdater, never()).accept(any(Device.class));
  }

  @Test
  void testRemoveDevice() {
    final Device primaryDevice = new Device();
    primaryDevice.setId(Device.PRIMARY_ID);

    final Device linkedDevice = new Device();
    linkedDevice.setId((byte) (Device.PRIMARY_ID + 1));

    Account account = AccountsHelper.generateTestAccount("+14152222222", List.of(primaryDevice, linkedDevice));

    when(accounts.getByAccountIdentifierAsync(account.getUuid()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(keysManager.deleteSingleUsePreKeys(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    assertTrue(account.getDevice(linkedDevice.getId()).isPresent());

    account = accountsManager.removeDevice(account, linkedDevice.getId()).join();

    assertFalse(account.getDevice(linkedDevice.getId()).isPresent());
    verify(messagesManager, times(2)).clear(account.getUuid(), linkedDevice.getId());
    verify(keysManager, times(2)).deleteSingleUsePreKeys(account.getUuid(), linkedDevice.getId());
    verify(keysManager).buildWriteItemsForRemovedDevice(account.getUuid(), account.getPhoneNumberIdentifier(), linkedDevice.getId());
    verify(clientPublicKeysManager).buildTransactWriteItemForDeletion(account.getUuid(), linkedDevice.getId());
    verify(clientPresenceManager).disconnectPresence(account.getUuid(), linkedDevice.getId());
  }

  @Test
  void testRemovePrimaryDevice() {
    final Device primaryDevice = new Device();
    primaryDevice.setId(Device.PRIMARY_ID);

    final Account account = AccountsHelper.generateTestAccount("+14152222222", List.of(primaryDevice));

    when(keysManager.deleteSingleUsePreKeys(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    assertThrows(IllegalArgumentException.class, () -> accountsManager.removeDevice(account, Device.PRIMARY_ID));

    assertDoesNotThrow(account::getPrimaryDevice);
    verify(messagesManager, never()).clear(any(), anyByte());
    verify(keysManager, never()).deleteSingleUsePreKeys(any(), anyByte());
    verify(clientPresenceManager, never()).disconnectPresence(any(), anyByte());
  }

  @Test
  void testCreateFreshAccount() throws InterruptedException, AccountAlreadyExistsException {
    when(accounts.create(any(), any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, true, null);

    final Account createdAccount = createAccount(e164, attributes);

    verify(accounts).create(argThat(account -> e164.equals(account.getNumber())), any());
    verify(keysManager).buildWriteItemsForNewDevice(
        eq(createdAccount.getUuid()),
        eq(createdAccount.getPhoneNumberIdentifier()),
        eq(Device.PRIMARY_ID),
        notNull(),
        notNull(),
        notNull(),
        notNull());

    verifyNoInteractions(messagesManager);
    verifyNoInteractions(profilesManager);
  }

  @Test
  void testReregisterAccount() throws InterruptedException, AccountAlreadyExistsException {
    final UUID existingUuid = UUID.randomUUID();

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, true, null);

    when(accounts.create(any(), any()))
        .thenAnswer(invocation -> {
          final Account requestedAccount = invocation.getArgument(0);

          final Account existingAccount = mock(Account.class);
          when(existingAccount.getUuid()).thenReturn(existingUuid);
          when(existingAccount.getIdentifier(IdentityType.ACI)).thenReturn(existingUuid);
          when(existingAccount.getNumber()).thenReturn(e164);
          when(existingAccount.getPhoneNumberIdentifier()).thenReturn(requestedAccount.getIdentifier(IdentityType.PNI));
          when(existingAccount.getIdentifier(IdentityType.PNI)).thenReturn(requestedAccount.getIdentifier(IdentityType.PNI));

          throw new AccountAlreadyExistsException(existingAccount);
        });

    when(accounts.reclaimAccount(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Account reregisteredAccount = createAccount(e164, attributes);

    assertTrue(phoneNumberIdentifiersByE164.containsKey(e164));

    verify(accounts)
        .create(argThat(account -> e164.equals(account.getNumber()) && existingUuid.equals(account.getUuid())), any());

    verify(keysManager).buildWriteItemsForNewDevice(
        eq(reregisteredAccount.getUuid()),
        eq(reregisteredAccount.getPhoneNumberIdentifier()),
        eq(Device.PRIMARY_ID),
        notNull(),
        notNull(),
        notNull(),
        notNull());

    verify(keysManager, times(2)).deleteSingleUsePreKeys(existingUuid);
    verify(keysManager, times(2)).deleteSingleUsePreKeys(phoneNumberIdentifiersByE164.get(e164));
    verify(messagesManager, times(2)).clear(existingUuid);
    verify(profilesManager, times(2)).deleteAll(existingUuid);
    verify(clientPresenceManager).disconnectAllPresencesForUuid(existingUuid);
  }

  @Test
  void testCreateAccountRecentlyDeleted() throws InterruptedException, AccountAlreadyExistsException {
    final UUID recentlyDeletedUuid = UUID.randomUUID();

    when(accounts.findRecentlyDeletedAccountIdentifier(anyString())).thenReturn(Optional.of(recentlyDeletedUuid));
    when(accounts.create(any(), any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, true, null);

    final Account account = createAccount(e164, attributes);

    verify(accounts).create(
        argThat(a -> e164.equals(a.getNumber()) && recentlyDeletedUuid.equals(a.getUuid())),
        any());

    verify(keysManager).buildWriteItemsForNewDevice(eq(account.getIdentifier(IdentityType.ACI)),
        eq(account.getIdentifier(IdentityType.PNI)),
        eq(Device.PRIMARY_ID),
        any(),
        any(),
        any(),
        any());

    verifyNoMoreInteractions(keysManager);
    verifyNoInteractions(messagesManager);
    verifyNoInteractions(profilesManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithDiscoverability(final boolean discoverable) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, discoverable, null);
    final Account account = createAccount("+18005550123", attributes);

    assertEquals(discoverable, account.isDiscoverableByPhoneNumber());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithStorageCapability(final boolean hasStorage) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null,
            true, new DeviceCapabilities(hasStorage, false, false, false, false));

    final Account account = createAccount("+18005550123", attributes);

    assertEquals(hasStorage, account.isStorageSupported());
  }

  @Test
  void testAddDevice() {
    final String phoneNumber =
        PhoneNumberUtil.getInstance().format(PhoneNumberUtil.getInstance().getExampleNumber("US"),
            PhoneNumberUtil.PhoneNumberFormat.E164);

    final Account account = AccountsHelper.generateTestAccount(phoneNumber, List.of(generateTestDevice(clock.millis())));
    final UUID aci = account.getIdentifier(IdentityType.ACI);
    final UUID pni = account.getIdentifier(IdentityType.PNI);

    final byte nextDeviceId = account.getNextDeviceId();

    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    final byte[] deviceNameCiphertext = "device-name".getBytes(StandardCharsets.UTF_8);
    final String password = "password";
    final String signalAgent = "OWT";
    final DeviceCapabilities deviceCapabilities = new DeviceCapabilities(true, true, true, false, false);
    final int aciRegistrationId = 17;
    final int pniRegistrationId = 19;
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    when(keysManager.deleteSingleUsePreKeys(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(accounts.getByAccountIdentifierAsync(aci)).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    when(accounts.updateTransactionallyAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    clock.pin(clock.instant().plusSeconds(60));

    final Pair<Account, Device> updatedAccountAndDevice = accountsManager.addDevice(account, new DeviceSpec(
            deviceNameCiphertext,
            password,
            signalAgent,
            deviceCapabilities,
            aciRegistrationId,
            pniRegistrationId,
            true,
            Optional.empty(),
            Optional.empty(),
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey))
        .join();

    verify(keysManager).deleteSingleUsePreKeys(aci, nextDeviceId);
    verify(keysManager).deleteSingleUsePreKeys(pni, nextDeviceId);
    verify(messagesManager).clear(aci, nextDeviceId);

    verify(keysManager).buildWriteItemsForNewDevice(
        aci,
        pni,
        nextDeviceId,
        aciSignedPreKey,
        pniSignedPreKey,
        aciPqLastResortPreKey,
        pniPqLastResortPreKey);

    final Device device = updatedAccountAndDevice.second();

    assertEquals(deviceNameCiphertext, device.getName());
    assertTrue(device.getAuthTokenHash().verify(password));
    assertEquals(signalAgent, device.getUserAgent());
    assertEquals(deviceCapabilities, device.getCapabilities());
    assertEquals(aciRegistrationId, device.getRegistrationId());
    assertEquals(pniRegistrationId, device.getPhoneNumberIdentityRegistrationId().getAsInt());
    assertTrue(device.getFetchesMessages());
    assertNull(device.getApnId());
    assertNull(device.getVoipApnId());
    assertNull(device.getGcmId());
  }

  @ParameterizedTest
  @MethodSource
  void testUpdateDeviceLastSeen(final boolean expectUpdate, final long initialLastSeen, final long updatedLastSeen) {
    final Account account = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
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

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account = accountsManager.changeNumber(account, targetNumber, null, null, null, null);

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    verify(keysManager).deleteSingleUsePreKeys(originalPni);
    verify(keysManager).deleteSingleUsePreKeys(phoneNumberIdentifiersByE164.get(targetNumber));
  }

  @Test
  void testChangePhoneNumberSameNumber() throws InterruptedException, MismatchedDevicesException {
    final String number = "+14152222222";

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account = accountsManager.changeNumber(account, number, null, null, null, null);

    assertEquals(number, account.getNumber());
    verify(keysManager, never()).deleteSingleUsePreKeys(any());
  }

  @Test
  void testChangePhoneNumberSameNumberWithPniData() {
    final String number = "+14152222222";

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    assertThrows(IllegalArgumentException.class,
        () -> accountsManager.changeNumber(
            account, number, new IdentityKey(Curve.generateKeyPair().getPublicKey()),
            Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair)), null, Map.of((byte) 1, 101)),
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

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account = accountsManager.changeNumber(account, targetNumber, null, null, null, null);

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));
    final UUID newPni = phoneNumberIdentifiersByE164.get(targetNumber);

    verify(keysManager).deleteSingleUsePreKeys(existingAccountUuid);
    verify(keysManager).deleteSingleUsePreKeys(originalPni);
    verify(keysManager, atLeastOnce()).deleteSingleUsePreKeys(targetPni);
    verify(keysManager).deleteSingleUsePreKeys(newPni);
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
    final byte deviceId2 = 2;
    final byte deviceId3 = 3;
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair),
        deviceId3, KeysHelper.signedECPreKey(3, identityKeyPair));
    final Map<Byte, KEMSignedPreKey> newSignedPqKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(4, identityKeyPair),
        deviceId2, KeysHelper.signedKEMPreKey(5, identityKeyPair),
        deviceId3, KeysHelper.signedKEMPreKey(6, identityKeyPair));
    final Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202, deviceId3, 203);

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));
    when(keysManager.getPqEnabledDevices(uuid)).thenReturn(CompletableFuture.completedFuture(List.of(Device.PRIMARY_ID, deviceId3)));
    when(keysManager.storePqLastResort(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final List<Device> devices = List.of(
        DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102),
        DevicesHelper.createDisabledDevice(deviceId3, 103));
    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final Account updatedAccount = accountsManager.changeNumber(
        account, targetNumber, new IdentityKey(Curve.generateKeyPair().getPublicKey()), newSignedKeys, newSignedPqKeys, newRegistrationIds);

    assertEquals(targetNumber, updatedAccount.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    final UUID newPni = phoneNumberIdentifiersByE164.get(targetNumber);
    verify(keysManager).deleteSingleUsePreKeys(existingAccountUuid);
    verify(keysManager, atLeastOnce()).deleteSingleUsePreKeys(targetPni);
    verify(keysManager).deleteSingleUsePreKeys(newPni);
    verify(keysManager).deleteSingleUsePreKeys(originalPni);
    verify(keysManager).getPqEnabledDevices(uuid);
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(newPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(newPni), eq(deviceId2), any());
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(newPni), eq(deviceId3), any());
    verify(keysManager).buildWriteItemForLastResortKey(eq(newPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager).buildWriteItemForLastResortKey(eq(newPni), eq(deviceId3), any());
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
    final byte deviceId2 = 2;
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Byte, KEMSignedPreKey> newSignedPqKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(3, identityKeyPair));
    final Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));
    when(keysManager.getPqEnabledDevices(uuid)).thenReturn(
        CompletableFuture.completedFuture(List.of(Device.PRIMARY_ID)));

    final List<Device> devices = List.of(DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));
    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
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

    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    assertThrows(AssertionError.class, () -> accountsManager.update(account, a -> a.setNumber(targetNumber, UUID.randomUUID())));
  }

  @Test
  void testPniUpdate() throws MismatchedDevicesException {
    final String number = "+14152222222";
    final byte deviceId2 = 2;

    List<Device> devices = List.of(DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair));
    Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    when(keysManager.getPqEnabledDevices(any())).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    when(keysManager.storeEcSignedPreKeys(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Account updatedAccount = accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, null, newRegistrationIds);

    // non-PNI stuff should not change
    assertEquals(oldUuid, updatedAccount.getUuid());
    assertEquals(number, updatedAccount.getNumber());
    assertEquals(oldPni, updatedAccount.getPhoneNumberIdentifier());
    assertNull(updatedAccount.getIdentityKey(IdentityType.ACI));
    assertEquals(Map.of(Device.PRIMARY_ID, 101, deviceId2, 102),
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::getRegistrationId)));

    // PNI stuff should
    assertEquals(pniIdentityKey, updatedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(newRegistrationIds,
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, d -> d.getPhoneNumberIdentityRegistrationId().getAsInt())));

    verify(accounts).updateTransactionallyAsync(any(), any());

    verify(keysManager).deleteSingleUsePreKeys(oldPni);
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(oldPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(oldPni), eq(deviceId2), any());
    verify(keysManager, never()).buildWriteItemForLastResortKey(any(), anyByte(), any());
  }

  @Test
  void testPniPqUpdate() throws MismatchedDevicesException {
    final String number = "+14152222222";
    final byte deviceId2 = 2;

    List<Device> devices = List.of(DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Byte, KEMSignedPreKey> newSignedPqKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(3, identityKeyPair),
        deviceId2, KeysHelper.signedKEMPreKey(4, identityKeyPair));
    Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();

    when(keysManager.getPqEnabledDevices(oldPni)).thenReturn(
        CompletableFuture.completedFuture(List.of(Device.PRIMARY_ID)));
    when(keysManager.storeEcSignedPreKeys(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.storePqLastResort(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    final Account updatedAccount =
        accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, newSignedPqKeys, newRegistrationIds);

    // non-PNI-keys stuff should not change
    assertEquals(oldUuid, updatedAccount.getUuid());
    assertEquals(number, updatedAccount.getNumber());
    assertEquals(oldPni, updatedAccount.getPhoneNumberIdentifier());
    assertNull(updatedAccount.getIdentityKey(IdentityType.ACI));
    assertEquals(Map.of(Device.PRIMARY_ID, 101, deviceId2, 102),
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::getRegistrationId)));

    // PNI keys should
    assertEquals(pniIdentityKey, updatedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(newRegistrationIds,
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, d -> d.getPhoneNumberIdentityRegistrationId().getAsInt())));

    verify(accounts).updateTransactionallyAsync(any(), any());

    verify(keysManager).deleteSingleUsePreKeys(oldPni);
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(oldPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(oldPni), eq(deviceId2), any());
    verify(keysManager).buildWriteItemForLastResortKey(eq(oldPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager, never()).buildWriteItemForLastResortKey(eq(oldPni), eq(deviceId2), any());
  }

  @Test
  void testPniNonPqToPqUpdate() throws MismatchedDevicesException {
    final String number = "+14152222222";
    final byte deviceId2 = 2;

    List<Device> devices = List.of(DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));

    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Byte, KEMSignedPreKey> newSignedPqKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(3, identityKeyPair),
        deviceId2, KeysHelper.signedKEMPreKey(4, identityKeyPair));
    Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    UUID oldUuid = account.getUuid();
    UUID oldPni = account.getPhoneNumberIdentifier();

    when(keysManager.getPqEnabledDevices(oldPni)).thenReturn(CompletableFuture.completedFuture(List.of()));
    when(keysManager.storeEcSignedPreKeys(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.storePqLastResort(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    final Account updatedAccount =
        accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, newSignedPqKeys, newRegistrationIds);

    // non-PNI-keys stuff should not change
    assertEquals(oldUuid, updatedAccount.getUuid());
    assertEquals(number, updatedAccount.getNumber());
    assertEquals(oldPni, updatedAccount.getPhoneNumberIdentifier());
    assertNull(updatedAccount.getIdentityKey(IdentityType.ACI));
    assertEquals(Map.of(Device.PRIMARY_ID, 101, deviceId2, 102),
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::getRegistrationId)));

    // PNI keys should
    assertEquals(pniIdentityKey, updatedAccount.getIdentityKey(IdentityType.PNI));
    assertEquals(newRegistrationIds,
        updatedAccount.getDevices().stream().collect(Collectors.toMap(Device::getId, d -> d.getPhoneNumberIdentityRegistrationId().getAsInt())));

    verify(accounts).updateTransactionallyAsync(any(), any());

    verify(keysManager).deleteSingleUsePreKeys(oldPni);
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(oldPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(oldPni), eq(deviceId2), any());
    verify(keysManager, never()).buildWriteItemForLastResortKey(any(), anyByte(), any());
  }

  @Test
  void testPniUpdate_incompleteKeys() {
    final String number = "+14152222222";
    final byte deviceId2 = 2;
    final byte deviceId3 = 3;
    List<Device> devices = List.of(DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));
    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        deviceId2, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId3, KeysHelper.signedECPreKey(2, identityKeyPair));
    Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    assertThrows(MismatchedDevicesException.class,
        () -> accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, null, newRegistrationIds));

    verifyNoInteractions(accounts);
    verifyNoInteractions(keysManager);
  }

  @Test
  void testPniPqUpdate_incompleteKeys() {
    final String number = "+14152222222";
    final byte deviceId2 = 2;
    List<Device> devices = List.of(DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));
    Account account = AccountsHelper.generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Byte, KEMSignedPreKey> newSignedPqKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(3, identityKeyPair));
    Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    assertThrows(MismatchedDevicesException.class,
        () -> accountsManager.updatePniKeys(account, pniIdentityKey, newSignedKeys, newSignedPqKeys, newRegistrationIds));

    verifyNoInteractions(accounts);
    verifyNoInteractions(keysManager);
  }

  @Test
  void testReserveUsernameHash() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByAccountIdentifierAsync(account.getUuid())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final List<byte[]> usernameHashes = List.of(TestRandomUtil.nextBytes(32), TestRandomUtil.nextBytes(32));
    when(accounts.reserveUsernameHash(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    UsernameReservation result = accountsManager.reserveUsernameHash(account, usernameHashes).join();
    assertArrayEquals(usernameHashes.get(0), result.reservedUsernameHash());
    verify(accounts, times(1)).reserveUsernameHash(eq(account), any(), eq(Duration.ofMinutes(5)));
  }

  @Test
  void testReserveOwnUsernameHash() {
    final byte[] oldUsernameHash = TestRandomUtil.nextBytes(32);
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account.setUsernameHash(oldUsernameHash);
    when(accounts.getByAccountIdentifierAsync(account.getUuid())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final List<byte[]> usernameHashes = List.of(TestRandomUtil.nextBytes(32), oldUsernameHash, TestRandomUtil.nextBytes(32));

    UsernameReservation result = accountsManager.reserveUsernameHash(account, usernameHashes).join();
    assertArrayEquals(oldUsernameHash, result.reservedUsernameHash());
    verify(accounts, never()).reserveUsernameHash(any(), any(), any());
  }

  @Test
  void testReserveUsernameOptimisticLockingFailure() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByAccountIdentifierAsync(account.getUuid())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final List<byte[]> usernameHashes = List.of(TestRandomUtil.nextBytes(32), TestRandomUtil.nextBytes(32));
    when(accounts.reserveUsernameHash(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new ContestedOptimisticLockException()))
        .thenReturn(CompletableFuture.completedFuture(null));

    UsernameReservation result = accountsManager.reserveUsernameHash(account, usernameHashes).join();
    assertArrayEquals(usernameHashes.get(0), result.reservedUsernameHash());
    verify(accounts, times(2)).reserveUsernameHash(eq(account), any(), eq(Duration.ofMinutes(5)));
  }

  @Test
  void testReserveUsernameHashNotAvailable() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByAccountIdentifierAsync(account.getUuid())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    when(accounts.reserveUsernameHash(any(), any(), any())).thenReturn(CompletableFuture.failedFuture(new UsernameHashNotAvailableException()));
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accountsManager.reserveUsernameHash(account, List.of(USERNAME_HASH_1, USERNAME_HASH_2)));
  }

  @Test
  void testConfirmReservedUsernameHash() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    setReservationHash(account, USERNAME_HASH_1);

    when(accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1))
        .thenReturn(CompletableFuture.completedFuture(null));

    accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    verify(accounts).confirmUsernameHash(eq(account), eq(USERNAME_HASH_1), eq(ENCRYPTED_USERNAME_1));
  }

  @Test
  void testConfirmReservedUsernameHashOptimisticLockingFailure() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    setReservationHash(account, USERNAME_HASH_1);
    when(accounts.getByAccountIdentifierAsync(account.getUuid())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1))
        .thenReturn(CompletableFuture.failedFuture(new ContestedOptimisticLockException()))
        .thenReturn(CompletableFuture.completedFuture(null));

    accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    verify(accounts, times(2)).confirmUsernameHash(eq(account), eq(USERNAME_HASH_1), eq(ENCRYPTED_USERNAME_1));
  }

  @Test
  void testConfirmReservedHashNameMismatch() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    setReservationHash(account, USERNAME_HASH_1);
    when(accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1))
        .thenReturn(CompletableFuture.completedFuture(null));
    CompletableFutureTestUtil.assertFailsWithCause(UsernameReservationNotFoundException.class,
        accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_2, ENCRYPTED_USERNAME_2));
  }

  @Test
  void testConfirmReservedLapsed() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    // hash was reserved, but the reservation lapsed and another account took it
    setReservationHash(account, USERNAME_HASH_1);
    when(accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1))
        .thenReturn(CompletableFuture.failedFuture(new UsernameHashNotAvailableException()));
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertTrue(account.getUsernameHash().isEmpty());
  }

  @Test
  void testConfirmReservedRetry() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account.setUsernameHash(USERNAME_HASH_1);

    // reserved username already set, should be treated as a replay
    accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    verifyNoInteractions(accounts);
  }

  @Test
  void testConfirmReservedUsernameHashWithNoReservation() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    CompletableFutureTestUtil.assertFailsWithCause(UsernameReservationNotFoundException.class,
        accountsManager.confirmReservedUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    verify(accounts, never()).confirmUsernameHash(any(), any(), any());
  }

  @Test
  void testClearUsernameHash() {
    when(accounts.clearUsernameHash(any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account.setUsernameHash(USERNAME_HASH_1);
    accountsManager.clearUsernameHash(account).join();
    verify(accounts).clearUsernameHash(eq(account));
  }

  @Test
  void testSetUsernameViaUpdate() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

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

    final Device originalDevice = originalAccount.getPrimaryDevice();
    final Device parsedDevice = parsedAccount.getPrimaryDevice();

    assertEquals(originalDevice.getId(), parsedDevice.getId());
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
    device.setId(Device.PRIMARY_ID);
    device.setFetchesMessages(true);
    device.setLastSeen(lastSeen);

    return device;
  }

  private Account createAccount(final String e164, final AccountAttributes accountAttributes) throws InterruptedException {
    final ECKeyPair aciKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniKeyPair = Curve.generateKeyPair();

    return accountsManager.create(e164,
        accountAttributes,
        new ArrayList<>(),
        new IdentityKey(aciKeyPair.getPublicKey()),
        new IdentityKey(pniKeyPair.getPublicKey()),
        new DeviceSpec(
            accountAttributes.getName(),
            "password",
            null,
            accountAttributes.getCapabilities(),
            accountAttributes.getRegistrationId(),
            accountAttributes.getPhoneNumberIdentityRegistrationId(),
            accountAttributes.getFetchesMessages(),
            Optional.empty(),
            Optional.empty(),
            KeysHelper.signedECPreKey(1, aciKeyPair),
            KeysHelper.signedECPreKey(2, pniKeyPair),
            KeysHelper.signedKEMPreKey(3, aciKeyPair),
            KeysHelper.signedKEMPreKey(4, pniKeyPair)),
        null);
  }
}
