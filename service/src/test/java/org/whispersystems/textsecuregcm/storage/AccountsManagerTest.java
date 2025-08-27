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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
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
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevices;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.RemoteAttachment;
import org.whispersystems.textsecuregcm.entities.TransferArchiveResult;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.AccountsManager.UsernameReservation;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.tests.util.MockRedisFuture;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.tests.util.RedisServerHelper;
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

  private static final byte[] LINK_DEVICE_SECRET = "link-device-secret".getBytes(StandardCharsets.UTF_8);

  private static TestClock CLOCK;

  private Accounts accounts;
  private KeysManager keysManager;
  private MessagesManager messagesManager;
  private ProfilesManager profilesManager;
  private DisconnectionRequestManager disconnectionRequestManager;
  private ClientPublicKeysManager clientPublicKeysManager;

  private Map<String, UUID> phoneNumberIdentifiersByE164;

  private RedisAsyncCommands<String, String> asyncCommands;
  private RedisAdvancedClusterCommands<String, String> clusterCommands;
  private RedisAdvancedClusterAsyncCommands<String, String> asyncClusterCommands;
  private AccountsManager accountsManager;
  private SecureValueRecoveryClient svr2Client;
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
  void setup() throws Exception {
    accounts = mock(Accounts.class);
    keysManager = mock(KeysManager.class);
    messagesManager = mock(MessagesManager.class);
    profilesManager = mock(ProfilesManager.class);
    disconnectionRequestManager = mock(DisconnectionRequestManager.class);
    clientPublicKeysManager = mock(ClientPublicKeysManager.class);
    dynamicConfiguration = mock(DynamicConfiguration.class);

    //noinspection unchecked
    asyncCommands = mock(RedisAsyncCommands.class);
    when(asyncCommands.set(any(), any(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    //noinspection unchecked
    clusterCommands = mock(RedisAdvancedClusterCommands.class);

    //noinspection unchecked
    asyncClusterCommands = mock(RedisAdvancedClusterAsyncCommands.class);
    when(asyncClusterCommands.del(any(String[].class))).thenReturn(MockRedisFuture.completedFuture(0L));
    when(asyncClusterCommands.get(any())).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncClusterCommands.set(any(), any(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

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
    when(storageClient.deleteStoredData(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));

    svr2Client = mock(SecureValueRecoveryClient.class);
    when(svr2Client.removeData(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    phoneNumberIdentifiersByE164 = new HashMap<>();

    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString())).thenAnswer((Answer<CompletableFuture<UUID>>) invocation -> {
      final String number = invocation.getArgument(0, String.class);
      return CompletableFuture.completedFuture(phoneNumberIdentifiersByE164.computeIfAbsent(number, n -> UUID.randomUUID()));
    });

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    final AccountLockManager accountLockManager = mock(AccountLockManager.class);

    doAnswer(invocation -> {
      final Callable<?> task = invocation.getArgument(1);
      return task.call();
    }).when(accountLockManager).withLock(anySet(), any(), any());

    when(accountLockManager.withLockAsync(anySet(), any(), any())).thenAnswer(invocation -> {
      final Supplier<CompletableFuture<?>> taskSupplier = invocation.getArgument(1);
      return taskSupplier.get();
    });

    final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        mock(RegistrationRecoveryPasswordsManager.class);

    when(registrationRecoveryPasswordsManager.remove(any())).thenReturn(CompletableFuture.completedFuture(null));

    when(keysManager.deleteSingleUsePreKeys(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(profilesManager.deleteAll(any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

    CLOCK = TestClock.now();

    final FaultTolerantRedisClient pubSubClient = RedisServerHelper.builder()
        .stringAsyncCommands(asyncCommands)
        .build();

    final FaultTolerantRedisClusterClient redisCluster = RedisClusterHelper.builder()
        .stringCommands(clusterCommands)
        .stringAsyncCommands(asyncClusterCommands)
        .build();

    when(disconnectionRequestManager.requestDisconnection(any())).thenReturn(CompletableFuture.completedFuture(null));

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        redisCluster,
        pubSubClient,
        accountLockManager,
        keysManager,
        messagesManager,
        profilesManager,
        storageClient,
        svr2Client,
        disconnectionRequestManager,
        registrationRecoveryPasswordsManager,
        clientPublicKeysManager,
        mock(Executor.class),
        mock(ScheduledExecutorService.class),
        mock(ScheduledExecutorService.class),
        CLOCK,
        LINK_DEVICE_SECRET,
        dynamicConfigurationManager);
  }

  @Test
  void testGetByServiceIdentifier() {
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    when(clusterCommands.get(eq("AccountMap::" + pni))).thenReturn(aci.toString());
    when(clusterCommands.get(eq("Account3::" + aci))).thenReturn(
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

    when(asyncClusterCommands.get(eq("AccountMap::" + pni))).thenReturn(MockRedisFuture.completedFuture(aci.toString()));
    when(asyncClusterCommands.get(eq("Account3::" + aci))).thenReturn(MockRedisFuture.completedFuture(
        "{\"number\": \"+14152222222\", \"pni\": \"" + pni + "\"}"));

    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

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

    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

    Optional<Account> account = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(clusterCommands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(clusterCommands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidInCacheAsync() {
    UUID uuid = UUID.randomUUID();

    when(asyncClusterCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}"));

    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    Optional<Account> account = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(asyncClusterCommands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(asyncClusterCommands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByPniInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    when(clusterCommands.get(eq("AccountMap::" + pni))).thenReturn(uuid.toString());
    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}");

    Optional<Account> account = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(clusterCommands).get(eq("AccountMap::" + pni));
    verify(clusterCommands).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(clusterCommands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByPniInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    when(asyncClusterCommands.get(eq("AccountMap::" + pni)))
        .thenReturn(MockRedisFuture.completedFuture(uuid.toString()));

    when(asyncClusterCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(
        "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}"));

    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    Optional<Account> account = accountsManager.getByPhoneNumberIdentifierAsync(pni).join();

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier());

    verify(asyncClusterCommands).get(eq("AccountMap::" + pni));
    verify(asyncClusterCommands).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(asyncClusterCommands);

    verifyNoInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidNotInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(clusterCommands, times(1)).get(eq("Account3::" + uuid));
    verify(clusterCommands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(clusterCommands, times(1)).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(clusterCommands);

    verify(accounts, times(1)).getByAccountIdentifier(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidNotInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncClusterCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncClusterCommands).get(eq("Account3::" + uuid));
    verify(asyncClusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncClusterCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncClusterCommands);

    verify(accounts).getByAccountIdentifierAsync(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniNotInCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(clusterCommands.get(eq("AccountMap::" + pni))).thenReturn(null);
    when(accounts.getByPhoneNumberIdentifier(pni)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(clusterCommands).get(eq("AccountMap::" + pni));
    verify(clusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(clusterCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(clusterCommands);

    verify(accounts).getByPhoneNumberIdentifier(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniNotInCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncClusterCommands.get(eq("AccountMap::" + pni))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByPhoneNumberIdentifierAsync(pni))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifierAsync(pni).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncClusterCommands).get(eq("AccountMap::" + pni));
    verify(asyncClusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncClusterCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncClusterCommands);

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

    when(clusterCommands.get(eq("Account3::" + uuid))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(clusterCommands, times(1)).get(eq("Account3::" + uuid));
    verify(clusterCommands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(clusterCommands, times(1)).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(clusterCommands);

    verify(accounts, times(1)).getByAccountIdentifier(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByUuidBrokenCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncClusterCommands.get(eq("Account3::" + uuid)))
        .thenReturn(MockRedisFuture.failedFuture(new RedisException("Connection lost!")));

    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncClusterCommands).get(eq("Account3::" + uuid));
    verify(asyncClusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncClusterCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncClusterCommands);

    verify(accounts).getByAccountIdentifierAsync(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniBrokenCache() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(clusterCommands.get(eq("AccountMap::" + pni))).thenThrow(new RedisException("OH NO"));
    when(accounts.getByPhoneNumberIdentifier(pni)).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifier(pni);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(clusterCommands).get(eq("AccountMap::" + pni));
    verify(clusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(clusterCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(clusterCommands);

    verify(accounts).getByPhoneNumberIdentifier(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testGetAccountByPniBrokenCacheAsync() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncClusterCommands.get(eq("AccountMap::" + pni)))
        .thenReturn(MockRedisFuture.failedFuture(new RedisException("OH NO")));

    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByPhoneNumberIdentifierAsync(pni))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByPhoneNumberIdentifierAsync(pni).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncClusterCommands).get(eq("AccountMap::" + pni));
    verify(asyncClusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    verify(asyncClusterCommands).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(asyncClusterCommands);

    verify(accounts).getByPhoneNumberIdentifierAsync(pni);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  void testUpdate_optimisticLockingFailure() {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.getByAccountIdentifier(uuid)).thenReturn(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])));
    doThrow(ContestedOptimisticLockException.class)
        .doAnswer(ACCOUNT_UPDATE_ANSWER)
        .when(accounts).update(any());

    final IdentityKey identityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

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

    when(asyncClusterCommands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.getByAccountIdentifierAsync(uuid)).thenReturn(CompletableFuture.completedFuture(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]))));

    when(accounts.updateAsync(any()))
        .thenReturn(CompletableFuture.failedFuture(new ContestedOptimisticLockException()))
        .thenAnswer(ACCOUNT_UPDATE_ASYNC_ANSWER);

    final IdentityKey identityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

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

    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(null);
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
    verify(disconnectionRequestManager).requestDisconnection(account.getUuid(), List.of(linkedDevice.getId()));
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
    verify(disconnectionRequestManager, never()).requestDisconnection(any(), any());
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

  @ParameterizedTest
  @CsvSource({
      "+18005550123, +18005550123",
      // the canonical form of numbers may change over time, so an existing account might have not-identical e164 that
      // maps to the same PNI, and the number used by the caller must be present on the re-registered account
      "+2290123456789, +22923456789"
  })
  void testReregisterAccount(final String e164, final String existingAccountE164)
      throws InterruptedException, AccountAlreadyExistsException {
    final UUID existingUuid = UUID.randomUUID();

    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, true, null);

    when(accounts.create(any(), any()))
        .thenAnswer(invocation -> {
          final Account requestedAccount = invocation.getArgument(0);

          final Account existingAccount = mock(Account.class);
          when(existingAccount.getUuid()).thenReturn(existingUuid);
          when(existingAccount.getIdentifier(IdentityType.ACI)).thenReturn(existingUuid);
          when(existingAccount.getNumber()).thenReturn(existingAccountE164);
          when(existingAccount.getPhoneNumberIdentifier()).thenReturn(requestedAccount.getIdentifier(IdentityType.PNI));
          when(existingAccount.getIdentifier(IdentityType.PNI)).thenReturn(requestedAccount.getIdentifier(IdentityType.PNI));
          when(existingAccount.getPrimaryDevice()).thenReturn(mock(Device.class));

          throw new AccountAlreadyExistsException(existingAccount);
        });

    when(accounts.reclaimAccount(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Account reregisteredAccount = createAccount(e164, attributes);

    assertTrue(phoneNumberIdentifiersByE164.containsKey(e164));
    assertEquals(e164, reregisteredAccount.getNumber());

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
    verify(profilesManager, times(2)).deleteAll(existingUuid, false);
    verify(disconnectionRequestManager).requestDisconnection(argThat(account ->
        account.getIdentifier(IdentityType.ACI).equals(existingUuid) && account != reregisteredAccount));
  }

  @Test
  void testCreateAccountRecentlyDeleted() throws InterruptedException, AccountAlreadyExistsException {
    final UUID recentlyDeletedUuid = UUID.randomUUID();

    when(accounts.findRecentlyDeletedAccountIdentifier(any())).thenReturn(Optional.of(recentlyDeletedUuid));
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
            true, hasStorage ? Set.of(DeviceCapability.STORAGE) : Set.of());

    final Account account = createAccount("+18005550123", attributes);

    assertEquals(hasStorage, account.hasCapability(DeviceCapability.STORAGE));
  }

  @Test
  void testAddDevice() {
    final String phoneNumber =
        PhoneNumberUtil.getInstance().format(PhoneNumberUtil.getInstance().getExampleNumber("US"),
            PhoneNumberUtil.PhoneNumberFormat.E164);

    final Account account = AccountsHelper.generateTestAccount(phoneNumber, List.of(generateTestDevice(CLOCK.millis())));
    final UUID aci = account.getIdentifier(IdentityType.ACI);
    final UUID pni = account.getIdentifier(IdentityType.PNI);
    account.setIdentityKey(new IdentityKey(ECKeyPair.generate().getPublicKey()));

    final byte nextDeviceId = account.getNextDeviceId();

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final byte[] deviceNameCiphertext = "device-name".getBytes(StandardCharsets.UTF_8);
    final String password = "password";
    final String signalAgent = "OWT";
    final Set<DeviceCapability> deviceCapabilities = Set.of();
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

    CLOCK.pin(CLOCK.instant().plusSeconds(60));

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
            pniPqLastResortPreKey),
            accountsManager.generateLinkDeviceToken(aci))
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
    assertEquals(Collections.emptySet(), device.getCapabilities());
    assertEquals(aciRegistrationId, device.getRegistrationId(IdentityType.ACI));
    assertEquals(pniRegistrationId, device.getRegistrationId(IdentityType.PNI));
    assertTrue(device.getFetchesMessages());
    assertNull(device.getApnId());
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

  @ParameterizedTest
  @CsvSource({
      "+14152222222,+14153333333",

      // Historically, "change number" behavior was different for "change to existing number," though that's no longer
      // the case
      "+14152222222,+14152222222"
  })
  void testChangePhoneNumber(final String originalNumber, final String targetNumber) throws InterruptedException, MismatchedDevicesException {
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(1, pniIdentityKeyPair);
    final KEMSignedPreKey kemLastResortPreKey = KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair);

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, List.of(DevicesHelper.createDevice(Device.PRIMARY_ID)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account = accountsManager.changeNumber(account,
        targetNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, ecSignedPreKey),
        Map.of(Device.PRIMARY_ID, kemLastResortPreKey),
        Map.of(Device.PRIMARY_ID, 1));

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    verify(keysManager).deleteSingleUsePreKeys(originalPni);
    verify(keysManager).deleteSingleUsePreKeys(phoneNumberIdentifiersByE164.get(targetNumber));
    verify(keysManager).buildWriteItemForEcSignedPreKey(phoneNumberIdentifiersByE164.get(targetNumber), Device.PRIMARY_ID, ecSignedPreKey);
    verify(keysManager).buildWriteItemForLastResortKey(phoneNumberIdentifiersByE164.get(targetNumber), Device.PRIMARY_ID, kemLastResortPreKey);
  }

  @Test
  void testChangePhoneNumberDifferentNumberSamePni() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+22923456789";
    // the canonical form of numbers may change over time, so we use PNIs as stable identifiers
    final String newNumber = "+2290123456789";
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final UUID phoneNumberIdentifier = UUID.randomUUID();

    Account account = AccountsHelper.generateTestAccount(originalNumber, UUID.randomUUID(), phoneNumberIdentifier,
        List.of(DevicesHelper.createDevice(Device.PRIMARY_ID)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    phoneNumberIdentifiersByE164.put(originalNumber, account.getPhoneNumberIdentifier());
    phoneNumberIdentifiersByE164.put(newNumber, account.getPhoneNumberIdentifier());
    account = accountsManager.changeNumber(account,
        newNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    assertEquals(newNumber, account.getNumber());
    assertEquals(phoneNumberIdentifier, account.getIdentifier(IdentityType.PNI));
    verify(accounts, never()).delete(any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccount() throws InterruptedException, MismatchedDevicesException {
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID existingAccountUuid = UUID.randomUUID();
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, List.of(DevicesHelper.createDevice(Device.PRIMARY_ID)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));

    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(1, pniIdentityKeyPair);
    final KEMSignedPreKey kemLastResoryPreKey = KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair);

    Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, List.of(DevicesHelper.createDevice(Device.PRIMARY_ID)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account = accountsManager.changeNumber(account,
        targetNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, ecSignedPreKey),
        Map.of(Device.PRIMARY_ID, kemLastResoryPreKey),
        Map.of(Device.PRIMARY_ID, 1));

    assertEquals(targetNumber, account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));
    final UUID newPni = phoneNumberIdentifiersByE164.get(targetNumber);

    verify(keysManager).deleteSingleUsePreKeys(existingAccountUuid);
    verify(keysManager).deleteSingleUsePreKeys(originalPni);
    verify(keysManager, atLeastOnce()).deleteSingleUsePreKeys(targetPni);
    verify(keysManager).deleteSingleUsePreKeys(newPni);
    verify(keysManager).buildWriteItemsForRemovedDevice(existingAccountUuid, targetPni, Device.PRIMARY_ID);
    verify(keysManager).buildWriteItemForEcSignedPreKey(newPni, Device.PRIMARY_ID, ecSignedPreKey);
    verify(keysManager).buildWriteItemForLastResortKey(newPni, Device.PRIMARY_ID, kemLastResoryPreKey);
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
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Byte, KEMSignedPreKey> newSignedPqKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(4, identityKeyPair),
        deviceId2, KeysHelper.signedKEMPreKey(5, identityKeyPair));
    final Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    final Account existingAccount = AccountsHelper.generateTestAccount(targetNumber, existingAccountUuid, targetPni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByE164(targetNumber)).thenReturn(Optional.of(existingAccount));
    when(keysManager.storePqLastResort(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final List<Device> devices = List.of(
        DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));
    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final Account updatedAccount = accountsManager.changeNumber(
        account, targetNumber, new IdentityKey(ECKeyPair.generate().getPublicKey()), newSignedKeys, newSignedPqKeys, newRegistrationIds);

    assertEquals(targetNumber, updatedAccount.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    final UUID newPni = phoneNumberIdentifiersByE164.get(targetNumber);
    verify(keysManager).deleteSingleUsePreKeys(existingAccountUuid);
    verify(keysManager, atLeastOnce()).deleteSingleUsePreKeys(targetPni);
    verify(keysManager).deleteSingleUsePreKeys(newPni);
    verify(keysManager).deleteSingleUsePreKeys(originalPni);
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(newPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager).buildWriteItemForEcSignedPreKey(eq(newPni), eq(deviceId2), any());
    verify(keysManager).buildWriteItemForLastResortKey(eq(newPni), eq(Device.PRIMARY_ID), any());
    verify(keysManager).buildWriteItemForLastResortKey(eq(newPni), eq(deviceId2), any());
    verifyNoMoreInteractions(keysManager);
  }


  @Test
  void testChangePhoneNumberWithMismatchedPqKeys() {
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID uuid = UUID.randomUUID();
    final UUID originalPni = UUID.randomUUID();
    final byte deviceId2 = 2;
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final Map<Byte, ECSignedPreKey> newSignedKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, identityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, identityKeyPair));
    final Map<Byte, KEMSignedPreKey> newSignedPqKeys = Map.of(
        Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(3, identityKeyPair));
    final Map<Byte, Integer> newRegistrationIds = Map.of(Device.PRIMARY_ID, 201, deviceId2, 202);

    final List<Device> devices = List.of(DevicesHelper.createDevice(Device.PRIMARY_ID, 0L, 101),
        DevicesHelper.createDevice(deviceId2, 0L, 102));
    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, originalPni, devices, new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    assertThrows(MismatchedDevicesException.class,
        () -> accountsManager.changeNumber(
            account, targetNumber, new IdentityKey(ECKeyPair.generate().getPublicKey()), newSignedKeys, newSignedPqKeys, newRegistrationIds));

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
  void testOnlyPrimaryCanWaitForDeviceLinked() {
    final Device primaryDevice = new Device();
    primaryDevice.setId(Device.PRIMARY_ID);

    final Device linkedDevice = new Device();
    linkedDevice.setId((byte) (Device.PRIMARY_ID + 1));

    final Account account = AccountsHelper.generateTestAccount("+14152222222", List.of(primaryDevice, linkedDevice));

    assertThrows(IllegalArgumentException.class,
        () -> accountsManager.waitForNewLinkedDevice(account.getUuid(), linkedDevice, "", Duration.ofSeconds(1)));

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
    assertEquals(originalDevice.getRegistrationId(IdentityType.ACI), parsedDevice.getRegistrationId(IdentityType.ACI));
    assertEquals(originalDevice.getRegistrationId(IdentityType.PNI), parsedDevice.getRegistrationId(IdentityType.PNI));
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
    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

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

  @Test
  void checkDeviceLinkingToken() {
    final UUID aci = UUID.randomUUID();

    assertEquals(Optional.of(aci),
        accountsManager.checkDeviceLinkingToken(accountsManager.generateLinkDeviceToken(aci)));
  }

  @ParameterizedTest
  @MethodSource
  void checkVerificationTokenBadToken(final String token, final Instant currentTime) {
    CLOCK.pin(currentTime);

    assertEquals(Optional.empty(), accountsManager.checkDeviceLinkingToken(token));
  }

  private static Stream<Arguments> checkVerificationTokenBadToken() throws InvalidKeyException {
    final Instant tokenTimestamp = Instant.now();

    return Stream.of(
        // Expired token
        Arguments.of(AccountsManager.generateLinkDeviceToken(UUID.randomUUID(),
                new SecretKeySpec(LINK_DEVICE_SECRET, AccountsManager.LINK_DEVICE_VERIFICATION_TOKEN_ALGORITHM),
                CLOCK),
            tokenTimestamp.plus(AccountsManager.LINK_DEVICE_TOKEN_EXPIRATION_DURATION).plusSeconds(1)),

        // Bad UUID
        Arguments.of("not-a-valid-uuid.1691096565171:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // No UUID
        Arguments.of(".1691096565171:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // Bad timestamp
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.not-a-valid-timestamp:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // No timestamp
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // Blank timestamp
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // No signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171", tokenTimestamp),

        // Blank signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171:", tokenTimestamp),

        // Incorrect signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171:0CKWF7q3E9fi4sB2or4q1A0Up2z_73EQlMAy7Dpel9c=", tokenTimestamp),

        // Invalid signature
        Arguments.of("e552603a-1492-4de6-872d-bac19a2825b4.1691096565171:This is not valid base64", tokenTimestamp)
    );
  }

  @ParameterizedTest
  @MethodSource
  void validateCompleteDeviceList(final Account account, final Set<Byte> deviceIds, @Nullable final MismatchedDevicesException expectedException) {
    final Executable validateCompleteDeviceListExecutable =
        () -> AccountsManager.validateCompleteDeviceList(account, deviceIds);

    if (expectedException != null) {
      final MismatchedDevicesException caughtException =
          assertThrows(MismatchedDevicesException.class, validateCompleteDeviceListExecutable);

      assertEquals(expectedException.getMismatchedDevices(), caughtException.getMismatchedDevices());
    } else {
      assertDoesNotThrow(validateCompleteDeviceListExecutable);
    }
  }

  @Test
  void testFirstSuccessfulTransferArchiveCompletableFutureOneTimeout() {
    // First future times out, second one completes successfully
    final RemoteAttachment transferArchive = new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)));

    final CompletableFuture<Optional<TransferArchiveResult>> timeoutFuture = new CompletableFuture<>();
    timeoutFuture.completeOnTimeout(Optional.empty(), 50, TimeUnit.MILLISECONDS);

    final CompletableFuture<Optional<TransferArchiveResult>> successfulFuture = new CompletableFuture<>();

    final CompletableFuture<Optional<TransferArchiveResult>> result =
        AccountsManager.firstSuccessfulTransferArchiveFuture(List.of(timeoutFuture, successfulFuture));

    CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
        .execute(() -> successfulFuture.complete(Optional.of(transferArchive)));

    final Optional<TransferArchiveResult> maybeTransferArchive = result.join();
    assertTrue(maybeTransferArchive.isPresent());
    assertEquals(transferArchive, maybeTransferArchive.get());
  }

  @Test
  void testFirstSuccessfulTransferArchiveCompletableFutureBothTimeout() {
    // Both futures time out
    final CompletableFuture<Optional<TransferArchiveResult>> firstTimeoutFuture = new CompletableFuture<>();
    firstTimeoutFuture.completeOnTimeout(Optional.empty(), 10, TimeUnit.MILLISECONDS);

    final CompletableFuture<Optional<TransferArchiveResult>> secondTimeoutFuture = new CompletableFuture<>();
    secondTimeoutFuture.completeOnTimeout(Optional.empty(), 10, TimeUnit.MILLISECONDS);

    final CompletableFuture<Optional<TransferArchiveResult>> result =
        AccountsManager.firstSuccessfulTransferArchiveFuture(List.of(firstTimeoutFuture, secondTimeoutFuture));

    assertTrue(result.join().isEmpty());
  }

  @Test
  void testFirstSuccessfulTransferArchiveCompletableFuture() {
    // First future completes successfully, second one times out
    final RemoteAttachment transferArchive = new RemoteAttachment(3, Base64.getUrlEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)));

    final CompletableFuture<Optional<TransferArchiveResult>> successfulFuture = new CompletableFuture<>();

    final CompletableFuture<Optional<TransferArchiveResult>> timeoutFuture = new CompletableFuture<>();
    timeoutFuture.completeOnTimeout(Optional.empty(), 50, TimeUnit.MILLISECONDS);

    final CompletableFuture<Optional<TransferArchiveResult>> result =
        AccountsManager.firstSuccessfulTransferArchiveFuture(List.of(successfulFuture, timeoutFuture));
    successfulFuture.complete(Optional.of(transferArchive));

    final Optional<TransferArchiveResult> maybeTransferArchive = result.join();
    assertTrue(maybeTransferArchive.isPresent());
    assertEquals(transferArchive, maybeTransferArchive.get());
  }

  private static List<Arguments> validateCompleteDeviceList() {
    final byte deviceId = Device.PRIMARY_ID;
    final byte extraDeviceId = deviceId + 1;

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);

    final Account account = mock(Account.class);
    when(account.getDevices()).thenReturn(List.of(device));

    return List.of(
        Arguments.of(account, Set.of(deviceId), null),

        Arguments.of(account, Set.of(deviceId, extraDeviceId),
            new MismatchedDevicesException(
                new MismatchedDevices(Collections.emptySet(), Set.of(extraDeviceId), Collections.emptySet()))),

        Arguments.of(account, Collections.emptySet(),
            new MismatchedDevicesException(
                new MismatchedDevices(Set.of(deviceId), Collections.emptySet(), Collections.emptySet()))),

        Arguments.of(account, Set.of(extraDeviceId),
            new MismatchedDevicesException(
                new MismatchedDevices(Set.of(deviceId), Set.of((byte) (extraDeviceId)), Collections.emptySet())))
    );
  }
}
