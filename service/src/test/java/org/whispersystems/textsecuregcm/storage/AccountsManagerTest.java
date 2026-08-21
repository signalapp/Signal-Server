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

import com.eatthepath.otp.TimeBasedOneTimePasswordGenerator;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevices;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
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
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.ThrowingSupplier;

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

  private static final Duration MAX_TOTP_VALIDATION_DELAY = AccountsManager.TOTP_PARAMETERS.timeStep().dividedBy(2);

  private static TestClock CLOCK;

  private Accounts accounts;
  private PhoneNumberIdentifiers phoneNumberIdentifiers;
  private KeysManager keysManager;
  private MessagesManager messagesManager;
  private ProfilesManager profilesManager;
  private DisconnectionRequestManager disconnectionRequestManager;
  private ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager;

  private Map<String, UUID> phoneNumberIdentifiersByE164;

  private RedisAsyncCommands<String, String> asyncCommands;
  private RedisAdvancedClusterCommands<String, String> clusterCommands;
  private RedisAdvancedClusterAsyncCommands<String, String> asyncClusterCommands;
  private AccountsManager accountsManager;
  private SecureValueRecoveryClient svr2Client;

  private static final Answer<?> ACCOUNT_UPDATE_ANSWER = (answer) -> {
    // it is implicit in the update() contract is that a successful call will
    // result in an incremented version
    final Account updatedAccount = answer.getArgument(0, Account.class);
    updatedAccount.setVersion(updatedAccount.getVersion() + 1);
    return null;
  };

  @BeforeEach
  void setup() throws Exception {
    accounts = mock(Accounts.class);
    keysManager = mock(KeysManager.class);
    messagesManager = mock(MessagesManager.class);
    profilesManager = mock(ProfilesManager.class);
    disconnectionRequestManager = mock(DisconnectionRequestManager.class);
    changeNumberWaitingPeriodManager = mock(ChangeNumberWaitingPeriodManager.class);

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

    phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    phoneNumberIdentifiersByE164 = new HashMap<>();

    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString())).thenAnswer((Answer<CompletableFuture<UUID>>) invocation -> {
      final String number = invocation.getArgument(0, String.class);
      return CompletableFuture.completedFuture(phoneNumberIdentifiersByE164.computeIfAbsent(number, _ -> UUID.randomUUID()));
    });

    final AccountLockManager accountLockManager = mock(AccountLockManager.class);

    doAnswer(invocation -> {
      final ThrowingSupplier<?, ?> task = invocation.getArgument(1);
      return task.get();
    }).when(accountLockManager).withLock(anySet(), any(), any());

    doAnswer(invocation -> {
      final ThrowingSupplier<?, ?> task = invocation.getArgument(1);
      return task.get();
    }).when(accountLockManager).withSingleAccountLock(any(Account.class), any(), any());

    final PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager =
        mock(PhoneNumberRecoveryPasswordsManager.class);

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
        changeNumberWaitingPeriodManager,
        storageClient,
        svr2Client,
        disconnectionRequestManager,
        phoneNumberRecoveryPasswordsManager,
        mock(Executor.class),
        mock(ScheduledExecutorService.class),
        mock(ScheduledExecutorService.class),
        CLOCK,
        LINK_DEVICE_SECRET,
        MAX_TOTP_VALIDATION_DELAY);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetByServiceIdentifier(final boolean hasNumber) {
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    final String accountJson = hasNumber
        ? "{\"number\": \"+14152222222\", \"pni\": \"" + pni + "\"}"
        : "{}";

    if (hasNumber) {
      when(clusterCommands.get(eq("AccountMap::" + pni))).thenReturn(aci.toString());
    }
    when(clusterCommands.get(eq("Account3::" + aci))).thenReturn(accountJson);

    if (hasNumber) {
      assertTrue(accountsManager.getByServiceIdentifier(new PniServiceIdentifier(pni)).isPresent());
      assertFalse(accountsManager.getByServiceIdentifier(new PniServiceIdentifier(aci)).isPresent());
    } else {
      verify(clusterCommands, never()).get(eq("AccountMap::" + pni));
    }

    assertTrue(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(aci)).isPresent());
    assertFalse(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(pni)).isPresent());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetByServiceIdentifierAsync(final boolean hasNumber) {
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    final String accountJson = hasNumber
        ? "{\"number\": \"+14152222222\", \"pni\": \"" + pni + "\"}"
        : "{}";

    when(asyncClusterCommands.get(eq("AccountMap::" + pni))).thenReturn(MockRedisFuture.completedFuture(aci.toString()));
    when(asyncClusterCommands.get(eq("Account3::" + aci))).thenReturn(MockRedisFuture.completedFuture(accountJson));

    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    when(accounts.getByAccountIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(accounts.getByPhoneNumberIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    if (hasNumber) {
      assertTrue(accountsManager.getByServiceIdentifierAsync(new PniServiceIdentifier(pni)).join().isPresent());
      assertFalse(accountsManager.getByServiceIdentifierAsync(new PniServiceIdentifier(aci)).join().isPresent());
    } else {
      verify(asyncClusterCommands, never()).get(eq("AccountMap::" + pni));
    }

    assertTrue(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(aci)).join().isPresent());
    assertFalse(accountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(pni)).join().isPresent());
  }


  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidInCache(final boolean hasNumber) {
    UUID uuid = UUID.randomUUID();

    final String accountJson = hasNumber
        ? "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}"
        : "{}";

    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(accountJson);

    Optional<Account> account = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(account.isPresent());
    assertEquals(account.get().getAccountIdentifier(), uuid);

    if (hasNumber) {
      assertEquals("+14152222222", account.get().getNumber().orElseThrow());
      assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier().orElseThrow());
    } else {
      assertTrue(account.get().getNumber().isEmpty());
      assertTrue(account.get().getPhoneNumberIdentifier().isEmpty());
    }

    verify(clusterCommands, times(1)).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(clusterCommands);

    verifyNoInteractions(accounts);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidInCacheAsync(final boolean hasNumber) {
    UUID uuid = UUID.randomUUID();

    final String accountJson = hasNumber
        ? "{\"number\": \"+14152222222\", \"pni\": \"de24dc73-fbd8-41be-a7d5-764c70d9da7e\"}"
        : "{}";

    when(asyncClusterCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(
        accountJson));

    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));

    Optional<Account> account = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(account.isPresent());
    assertEquals(account.get().getAccountIdentifier(), uuid);

    if (hasNumber) {
      assertEquals( "+14152222222", account.get().getNumber().orElseThrow());
      assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier().orElseThrow());
    } else {
      assertTrue(account.get().getNumber().isEmpty());
      assertTrue(account.get().getPhoneNumberIdentifier().isEmpty());
    }

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
    assertEquals("+14152222222", account.get().getNumber().orElseThrow());
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier().orElseThrow());

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
    assertEquals("+14152222222", account.get().getNumber().orElseThrow());
    assertEquals(UUID.fromString("de24dc73-fbd8-41be-a7d5-764c70d9da7e"), account.get().getPhoneNumberIdentifier().orElseThrow());

    verify(asyncClusterCommands).get(eq("AccountMap::" + pni));
    verify(asyncClusterCommands).get(eq("Account3::" + uuid));
    verifyNoMoreInteractions(asyncClusterCommands);

    verifyNoInteractions(accounts);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidNotInCache(final boolean hasNumber) {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    final Account account = hasNumber
        ? AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])
        : AccountsHelper.generateTestAccount(null, uuid, null, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    final Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(clusterCommands, times(1)).get(eq("Account3::" + uuid));
    if (hasNumber) {
      verify(clusterCommands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    } else {
      verify(clusterCommands, never()).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    }
    verify(clusterCommands, times(1)).setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(clusterCommands);

    verify(accounts, times(1)).getByAccountIdentifier(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidNotInCacheAsync(final boolean hasNumber) {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    final Account account = hasNumber
        ? AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])
        : AccountsHelper.generateTestAccount(null, uuid, null, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncClusterCommands.get(eq("Account3::" + uuid))).thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncClusterCommands).get(eq("Account3::" + uuid));
    if (hasNumber) {
      verify(asyncClusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    } else {
      verify(asyncClusterCommands, never()).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    }

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUsernameHash(final boolean hasNumber) {
    UUID uuid = UUID.randomUUID();
    final Account account = hasNumber
        ? AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])
        : AccountsHelper.generateTestAccount(null, uuid, null, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account.setUsernameHash(USERNAME_HASH_1);
    when(accounts.getByUsernameHash(USERNAME_HASH_1))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    Optional<Account> retrieved = accountsManager.getByUsernameHash(USERNAME_HASH_1).join();
    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);
    verify(accounts).getByUsernameHash(USERNAME_HASH_1);
    verifyNoMoreInteractions(accounts);
  }

  enum FailureStep {
    GET,
    SET_ACI,
    SET_PNI
  }

  @CartesianTest
  void testGetAccountByUuidBrokenCache(
      @CartesianTest.Enum(FailureStep.class) final FailureStep step,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasNumber) {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    final Account account = hasNumber
        ? AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])
        : AccountsHelper.generateTestAccount(null, uuid, null, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    (switch (step) {
      case GET -> when(clusterCommands.get(eq("Account3::" + uuid)));
      case SET_ACI -> when(clusterCommands.setex(eq("Account3::" + uuid), anyLong(), anyString()));
      case SET_PNI -> when(clusterCommands.setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString())));
    }).thenThrow(new RedisException("Connection lost!"));

    when(accounts.getByAccountIdentifier(eq(uuid))).thenReturn(Optional.of(account));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifier(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(clusterCommands, times(1)).get(eq("Account3::" + uuid));
    if (hasNumber) {
      verify(clusterCommands, times(1)).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    } else {
      verify(clusterCommands, never()).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    }
    // If the account has a number, we only try setting the ACI if we successfully set the PNI.
    verify(clusterCommands, times(step == FailureStep.SET_PNI && hasNumber ? 0 : 1))
        .setex(eq("Account3::" + uuid), anyLong(), anyString());
    verifyNoMoreInteractions(clusterCommands);

    verify(accounts, times(1)).getByAccountIdentifier(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @CartesianTest
  void testGetAccountByUuidBrokenCacheAsync(
      @CartesianTest.Enum(FailureStep.class) final FailureStep step,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasNumber) {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    final Account account = hasNumber
        ? AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])
        : AccountsHelper.generateTestAccount(null, uuid, null, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);

    when(asyncClusterCommands.get(eq("Account3::" + uuid)))
        .thenReturn(MockRedisFuture.completedFuture(null));
    when(asyncClusterCommands.setex(any(), anyLong(), any())).thenReturn(MockRedisFuture.completedFuture("OK"));
    when(accounts.getByAccountIdentifierAsync(eq(uuid)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    (switch (step) {
      case GET -> when(asyncClusterCommands.get(eq("Account3::" + uuid)));
      case SET_ACI -> when(asyncClusterCommands.setex(eq("Account3::" + uuid), anyLong(), anyString()));
      case SET_PNI -> when(asyncClusterCommands.setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString())));
    }).thenReturn(MockRedisFuture.failedFuture(new RedisException("Connection lost!")));

    Optional<Account> retrieved = accountsManager.getByAccountIdentifierAsync(uuid).join();

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(asyncClusterCommands).get(eq("Account3::" + uuid));
    if (hasNumber) {
      verify(asyncClusterCommands).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    } else {
      verify(asyncClusterCommands, never()).setex(eq("AccountMap::" + pni), anyLong(), eq(uuid.toString()));
    }
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdate_optimisticLockingFailure(final boolean numberless) {
    UUID uuid = UUID.randomUUID();
    UUID pni = UUID.randomUUID();
    Account account = numberless
        ? AccountsHelper.generateTestAccountNoPhoneNumber(new ArrayList<>())
        : AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);

    when(clusterCommands.get(eq("Account3::" + uuid))).thenReturn(null);

    when(accounts.getByAccountIdentifier(uuid)).thenReturn(
        Optional.of(AccountsHelper.generateTestAccount("+14152222222", uuid, pni, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])));
    doThrow(ContestedOptimisticLockException.class)
        .doAnswer(ACCOUNT_UPDATE_ANSWER)
        .when(accounts).update(any());

    final IdentityKey identityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

    account = accountsManager.update(uuid, a -> a.setIdentityKey(identityKey));

    assertEquals(1, account.getVersion());
    assertEquals(identityKey, account.getAccountIdentityKey());

    verify(accounts, times(2)).getByAccountIdentifier(uuid);
    verify(accounts, times(2)).update(any());
    verifyNoMoreInteractions(accounts);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdateDevice(final boolean numberless) {
    final UUID uuid = UUID.randomUUID();
    Account account = numberless
        ? AccountsHelper.generateTestAccount(null, uuid, null, new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH])
        : AccountsHelper.generateTestAccount("+14152222222", uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);

    assertTrue(account.getDevices().isEmpty());

    Device enabledDevice = new Device();
    enabledDevice.setFetchesMessages(true);
    enabledDevice.setLastSeen(System.currentTimeMillis());
    final byte deviceId = account.getNextDeviceId();
    enabledDevice.setId(deviceId);
    account.addDevice(enabledDevice);

    @SuppressWarnings("unchecked") Consumer<Device> deviceUpdater = mock(Consumer.class);
    @SuppressWarnings("unchecked") Consumer<Device> unknownDeviceUpdater = mock(Consumer.class);

    accountsManager.updateDevice(uuid, deviceId, deviceUpdater);
    account = accountsManager.updateDevice(uuid, deviceId, d -> d.setName("deviceName".getBytes(StandardCharsets.UTF_8)));

    assertArrayEquals("deviceName".getBytes(StandardCharsets.UTF_8), account.getDevice(deviceId).orElseThrow().getName());

    verify(deviceUpdater, times(1)).accept(any(Device.class));

    accountsManager.updateDevice(uuid, account.getNextDeviceId(), unknownDeviceUpdater);

    verify(unknownDeviceUpdater, never()).accept(any(Device.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRemoveDevice(final boolean numberless) {
    final Device primaryDevice = new Device();
    primaryDevice.setId(Device.PRIMARY_ID);

    final Device linkedDevice = new Device();
    linkedDevice.setId((byte) (Device.PRIMARY_ID + 1));

   Account account = AccountsHelper.generateTestAccount(
        numberless ? null : "+14152222222",
        UUID.randomUUID(),
        numberless ? null : UUID.randomUUID(),
        List.of(primaryDevice, linkedDevice),
        new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]
    );

    when(accounts.getByAccountIdentifier(account.getAccountIdentifier())).thenReturn(Optional.of(account));
    when(keysManager.deleteSingleUsePreKeys(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    assertTrue(account.getDevice(linkedDevice.getId()).isPresent());

    account = accountsManager.removeDevice(account.getAccountIdentifier(), linkedDevice.getId());

    final UUID aci = account.getAccountIdentifier();
    assertFalse(account.getDevice(linkedDevice.getId()).isPresent());
    verify(messagesManager, times(2)).clear(aci, linkedDevice.getId());
    verify(keysManager, times(2)).deleteSingleUsePreKeys(aci, linkedDevice.getId());

    if (numberless) {
      verify(keysManager, never()).deleteSingleUsePreKeys(argThat(id -> !id.equals(aci)), anyByte());
    } else {
      //noinspection OptionalGetWithoutIsPresent
      verify(keysManager, times(2)).deleteSingleUsePreKeys(eq(account.getPhoneNumberIdentifier().get()), eq(linkedDevice.getId()));
    }

    verify(keysManager).buildWriteItemsForRemovedDevice(aci, account.getPhoneNumberIdentifier(), linkedDevice.getId());
    verify(disconnectionRequestManager).requestDisconnection(aci, List.of(linkedDevice.getId()));
  }

  @Test
  void testRemovePrimaryDevice() {
    final Device primaryDevice = new Device();
    primaryDevice.setId(Device.PRIMARY_ID);

    final Account account = AccountsHelper.generateTestAccount("+14152222222", List.of(primaryDevice));

    when(keysManager.deleteSingleUsePreKeys(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(messagesManager.clear(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    assertThrows(IllegalArgumentException.class,
        () -> accountsManager.removeDevice(account.getAccountIdentifier(), Device.PRIMARY_ID));

    assertDoesNotThrow(account::getPrimaryDevice);
    verify(messagesManager, never()).clear(any(), anyByte());
    verify(keysManager, never()).deleteSingleUsePreKeys(any(), anyByte());
    verify(disconnectionRequestManager, never()).requestDisconnection(any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateFreshAccount(final boolean hasE164)
      throws AccountAlreadyExistsException, ReceiptAlreadyRedeemedException {
    when(accounts.create(any(), any())).thenReturn(true);

    final Optional<String> maybeE164 = hasE164 ? Optional.of("+18005550123") : Optional.empty();
    final Integer pniRegistrationId = hasE164 ? 2 : null;
    final AccountAttributes attributes = new AccountAttributes(false, 1, pniRegistrationId, null, null, hasE164, null,
        TestRandomUtil.nextBytes(16));

    final Account createdAccount = maybeE164.isPresent()
        ? createAccount(maybeE164.get(), attributes)
        : createAccount(attributes);

    // Check existence (or lack thereof) of phone number, phone number identifier, phone number identity key,
    // phone number identity registration ID, and auth credential salt.
    final Device primaryDevice = createdAccount.getDevices().stream().findFirst().orElseThrow();
    assertEquals(maybeE164, createdAccount.getNumber());
    maybeE164.ifPresentOrElse(
        number -> {
          assertTrue(phoneNumberIdentifiersByE164.containsKey(number));
          assertTrue(createdAccount.getPhoneNumberIdentityKey().isPresent());
          assertEquals(pniRegistrationId, primaryDevice.getPhoneNumberIdentityRegistrationId().orElseThrow());
          assertTrue(createdAccount.getAuthCredentialSalt().isEmpty());
        },
        () -> {
          assertTrue(phoneNumberIdentifiersByE164.isEmpty());
          assertTrue(createdAccount.getPhoneNumberIdentityKey().isEmpty());
          assertTrue(primaryDevice.getPhoneNumberIdentityRegistrationId().isEmpty());
          assertTrue(createdAccount.getAuthCredentialSalt().isPresent());
        });

    if (maybeE164.isPresent()) {
      verify(accounts).create(argThat(account -> maybeE164.equals(account.getNumber())), any());
    } else {
      verify(accounts).create(argThat(account -> account.getNumber().isEmpty()), any(), any(), any());
    }
    verify(keysManager).buildWriteItemsForNewDevice(
        eq(createdAccount.getAccountIdentifier()),
        eq(createdAccount.getPhoneNumberIdentifier()),
        eq(Device.PRIMARY_ID),
        notNull(),
        maybeE164.isPresent() ? notNull() : eq(Optional.empty()),
        notNull(),
        maybeE164.isPresent() ? notNull() : eq(Optional.empty()));

    verify(changeNumberWaitingPeriodManager).handleAccountCreated(eq(createdAccount.getAccountIdentifier()), any(Instant.class));

    verifyNoInteractions(messagesManager);
    verifyNoInteractions(profilesManager);
  }

  @ParameterizedTest
  @MethodSource
  void testReregisterAccount(
      final Optional<String> maybeE164,
      final Optional<String> maybeExistingAccountE164)
      throws AccountAlreadyExistsException, ReceiptAlreadyRedeemedException {
    final UUID existingUuid = UUID.randomUUID();
    final Integer pniRegistrationId = maybeE164.isPresent() ? 2 : null;
    final AccountAttributes attributes = new AccountAttributes(false, 1, pniRegistrationId, null, null, maybeE164.isPresent(), null,
        null);
    final byte[] recoveryPassword = TestRandomUtil.nextBytes(32);
    attributes.setRecoveryPassword(recoveryPassword);

    final Answer<Boolean> existingAccountAnswer = invocation -> {
      final Account requestedAccount = invocation.getArgument(0);

      final Device existingPrimaryDevice = mock(Device.class);

      final Account existingAccount = mock(Account.class);
      when(existingAccount.getAccountIdentifier()).thenReturn(existingUuid);
      when(existingAccount.getNumber()).thenReturn(maybeExistingAccountE164);
      when(existingAccount.getPhoneNumberIdentifier()).thenReturn(requestedAccount.getPhoneNumberIdentifier());
      when(existingAccount.getPrimaryDevice()).thenReturn(existingPrimaryDevice);
      when(existingAccount.getAccountIdentityKey()).thenReturn(requestedAccount.getAccountIdentityKey());
      when(existingAccount.getAccountRecoveryPassword()).thenReturn(
          Optional.of(SaltedTokenHash.generateFor(HexFormat.of().formatHex(recoveryPassword))));

      throw new AccountAlreadyExistsException(existingAccount);
    };

    if (maybeE164.isPresent()) {
      when(accounts.create(any(), any())).thenAnswer(existingAccountAnswer);
    } else {
      when(accounts.create(any(), any(), any(), any())).thenAnswer(existingAccountAnswer);
    }

    when(accounts.reclaimAccount(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Account reregisteredAccount = maybeE164.isPresent()
        ? createAccount(maybeE164.get(), attributes)
        : createAccount(attributes);

    // Check existence (or lack thereof) of phone number, phone number identifier, phone number identity key,
    // and phone number identity registration ID
    final Device primaryDevice = reregisteredAccount.getDevices().stream().findFirst().orElseThrow();

    assertEquals(maybeE164, reregisteredAccount.getNumber());
    maybeE164.ifPresentOrElse(
        number -> {
          assertTrue(phoneNumberIdentifiersByE164.containsKey(number));
          assertTrue(reregisteredAccount.getPhoneNumberIdentityKey().isPresent());
          assertEquals(pniRegistrationId, primaryDevice.getPhoneNumberIdentityRegistrationId().orElseThrow());
        },
        () -> {
          assertTrue(phoneNumberIdentifiersByE164.isEmpty());
          assertTrue(reregisteredAccount.getPhoneNumberIdentityKey().isEmpty());
          assertTrue(primaryDevice.getPhoneNumberIdentityRegistrationId().isEmpty());
        });

    if (maybeE164.isPresent()) {
      verify(accounts)
          .create(argThat(account -> existingUuid.equals(account.getAccountIdentifier())), any());
    } else {
      verify(accounts)
          .create(argThat(account -> existingUuid.equals(account.getAccountIdentifier())), any(), any(), any());
    }

    verify(keysManager).buildWriteItemsForNewDevice(
        eq(reregisteredAccount.getAccountIdentifier()),
        eq(reregisteredAccount.getPhoneNumberIdentifier()),
        eq(Device.PRIMARY_ID),
        notNull(),
        maybeE164.isPresent() ? notNull() : eq(Optional.empty()),
        notNull(),
        maybeE164.isPresent() ? notNull() : eq(Optional.empty()));

    verify(keysManager, times(2)).deleteSingleUsePreKeys(existingUuid);
    maybeE164.ifPresent(number -> verify(keysManager, times(2)).deleteSingleUsePreKeys(phoneNumberIdentifiersByE164.get(number)));
    verify(messagesManager, times(2)).clear(existingUuid);
    verify(profilesManager, times(2)).deleteAll(existingUuid, false);
    verify(disconnectionRequestManager).requestDisconnection(argThat(account ->
        account.getAccountIdentifier().equals(existingUuid) && account != reregisteredAccount));
    verify(changeNumberWaitingPeriodManager).handleAccountCreated(eq(existingUuid), any(Instant.class));
  }

  private static List<Arguments> testReregisterAccount() {
    return List.of(
        Arguments.argumentSet("Re-register with the same phone number", Optional.of("+18005550123"), Optional.of("+18005550123")),
        // the canonical form of numbers may change over time, so an existing account might have not-identical e164 that
        // maps to the same PNI, and the number used by the caller must be present on the re-registered account
        Arguments.argumentSet("Re-register with a phone number in the same equivalence class", Optional.of("+2290123456789"), Optional.of("+22923456789")),
        Arguments.argumentSet("Re-register a numberless account", Optional.empty(), Optional.empty()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void reclaim(final boolean hasPhoneNumber) {
    final Account existingAccount = new Account();
    existingAccount.setAccountIdentifier(UUID.randomUUID());

    final Device existingPrimaryDevice = new Device();
    existingPrimaryDevice.setId(Device.PRIMARY_ID);

    existingAccount.addDevice(existingPrimaryDevice);

    final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

    existingAccount.setAccountRecoveryPassword(recoveryPassword);

    final int aciRegistrationId = 17;
    final int pniRegistrationId = 19;

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciKeyPair.getPublicKey());
    final IdentityKey pniIdentityKey = new IdentityKey(pniKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    if (hasPhoneNumber) {
      existingAccount.setNumber(PhoneNumberUtil.getInstance().format(
          PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164),
          UUID.randomUUID());
    }

    final AccountAttributes accountAttributes = new AccountAttributes(false,
        aciRegistrationId,
        hasPhoneNumber ? pniRegistrationId : null,
        TestRandomUtil.nextBytes(16),
        hasPhoneNumber ? "registration-lock" : null,
        hasPhoneNumber,
        Collections.emptySet(),
        recoveryPassword);

    final DeviceSpec primaryDeviceSpec = new DeviceSpec(
        TestRandomUtil.nextBytes(16),
        RandomStringUtils.insecure().nextAlphanumeric(12),
        "test",
        Collections.emptySet(),
        new DeviceIdentityInfo(aciRegistrationId, aciSignedPreKey, aciPqLastResortPreKey),
        Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)).filter(_ -> hasPhoneNumber),
        true,
        Optional.empty(),
        Optional.empty());

    when(accounts.reclaimAccount(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Account reclaimedAccount = accountsManager.recover(existingAccount,
        accountAttributes,
        aciIdentityKey,
        hasPhoneNumber ? Optional.of(pniIdentityKey) : Optional.empty(),
        primaryDeviceSpec,
        null);

    assertEquals(existingAccount.getAccountIdentifier(), reclaimedAccount.getAccountIdentifier());
    assertEquals(existingAccount.getNumber(), reclaimedAccount.getNumber());
    assertEquals(existingAccount.getPhoneNumberIdentifier(), reclaimedAccount.getPhoneNumberIdentifier());
    assertEquals(aciIdentityKey, reclaimedAccount.getAccountIdentityKey());
    assertEquals(hasPhoneNumber ? Optional.of(pniIdentityKey) : Optional.empty(), reclaimedAccount.getPhoneNumberIdentityKey());

    final Device reclaimedPrimaryDevice = reclaimedAccount.getPrimaryDevice();
    assertArrayEquals(primaryDeviceSpec.deviceNameCiphertext(), reclaimedPrimaryDevice.getName());
    assertEquals(primaryDeviceSpec.signalAgent(), reclaimedPrimaryDevice.getUserAgent());
    assertEquals(aciRegistrationId, reclaimedPrimaryDevice.getAccountRegistrationId());
    assertEquals(hasPhoneNumber ? Optional.of(pniRegistrationId) : Optional.empty(), reclaimedPrimaryDevice.getPhoneNumberIdentityRegistrationId());
    assertTrue(reclaimedPrimaryDevice.getFetchesMessages());
    assertTrue(StringUtils.isBlank(reclaimedPrimaryDevice.getApnId()));
    assertTrue(StringUtils.isBlank(reclaimedPrimaryDevice.getGcmId()));

    assertTrue(reclaimedAccount.getAccountRecoveryPassword().orElseThrow().verify(HexFormat.of().formatHex(recoveryPassword)));
    assertTrue(reclaimedPrimaryDevice.getAuthTokenHash().verify(primaryDeviceSpec.password()));

    verify(accounts).reclaimAccount(eq(existingAccount), argThat(account -> existingAccount.getAccountIdentifier().equals(account.getAccountIdentifier())), any());

    verify(keysManager).buildWriteItemsForNewDevice(
        eq(reclaimedAccount.getAccountIdentifier()),
        eq(reclaimedAccount.getPhoneNumberIdentifier()),
        eq(Device.PRIMARY_ID),
        notNull(),
        hasPhoneNumber ? notNull() : eq(Optional.empty()),
        notNull(),
        hasPhoneNumber ? notNull() : eq(Optional.empty()));

    verify(keysManager, times(2)).deleteSingleUsePreKeys(existingAccount.getAccountIdentifier());
    existingAccount.getPhoneNumberIdentifier().ifPresent(phoneNumberIdentifier ->
        verify(keysManager, times(2)).deleteSingleUsePreKeys(phoneNumberIdentifier));
    verify(messagesManager, times(2)).clear(existingAccount.getAccountIdentifier());
    verify(profilesManager, times(2)).deleteAll(existingAccount.getAccountIdentifier(), false);
    verify(disconnectionRequestManager).requestDisconnection(argThat(account ->
        account.getAccountIdentifier().equals(existingAccount.getAccountIdentifier()) && account != reclaimedAccount));
    verify(changeNumberWaitingPeriodManager).handleAccountCreated(eq(existingAccount.getAccountIdentifier()), any(Instant.class));
  }

  @Test
  void reclaimPniPresenceMismatch() {
    final Account existingAccount = new Account();
    existingAccount.setAccountIdentifier(UUID.randomUUID());

    final Device existingPrimaryDevice = new Device();
    existingPrimaryDevice.setId(Device.PRIMARY_ID);

    existingAccount.addDevice(existingPrimaryDevice);

    final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

    existingAccount.setAccountRecoveryPassword(recoveryPassword);

    final int aciRegistrationId = 17;
    final int pniRegistrationId = 19;

    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciKeyPair.getPublicKey());
    final IdentityKey pniIdentityKey = new IdentityKey(pniKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    existingAccount.setNumber(PhoneNumberUtil.getInstance().format(
            PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164),
        UUID.randomUUID());

    {
      final AccountAttributes accountAttributes = new AccountAttributes(false,
          aciRegistrationId,
          pniRegistrationId,
          TestRandomUtil.nextBytes(16),
          "registration-lock",
          true,
          Collections.emptySet(),
          recoveryPassword);

      final DeviceSpec primaryDeviceSpec = new DeviceSpec(
          TestRandomUtil.nextBytes(16),
          RandomStringUtils.insecure().nextAlphanumeric(12),
          "test",
          Collections.emptySet(),
          new DeviceIdentityInfo(aciRegistrationId, aciSignedPreKey, aciPqLastResortPreKey),
          Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)),
          true,
          Optional.empty(),
          Optional.empty());

      assertThrows(IllegalArgumentException.class, () -> accountsManager.recover(existingAccount,
          accountAttributes,
          aciIdentityKey,
          Optional.empty(),
          primaryDeviceSpec,
          null));
    }

    existingAccount.setNumber(null, null);

    {
      final AccountAttributes accountAttributes = new AccountAttributes(false,
          aciRegistrationId,
          null,
          TestRandomUtil.nextBytes(16),
          null,
          false,
          Collections.emptySet(),
          recoveryPassword);

      final DeviceSpec primaryDeviceSpec = new DeviceSpec(
          TestRandomUtil.nextBytes(16),
          RandomStringUtils.insecure().nextAlphanumeric(12),
          "test",
          Collections.emptySet(),
          new DeviceIdentityInfo(aciRegistrationId, aciSignedPreKey, aciPqLastResortPreKey),
          Optional.empty(),
          true,
          Optional.empty(),
          Optional.empty());

      assertThrows(IllegalArgumentException.class, () -> accountsManager.recover(existingAccount,
          accountAttributes,
          aciIdentityKey,
          Optional.of(pniIdentityKey),
          primaryDeviceSpec,
          null));
    }
  }

  @Test
  void testCreateAccountRecentlyDeleted() throws InterruptedException, AccountAlreadyExistsException {
    final UUID recentlyDeletedUuid = UUID.randomUUID();

    when(accounts.findRecentlyDeletedAccountIdentifier(any())).thenReturn(Optional.of(recentlyDeletedUuid));
    when(accounts.create(any(), any())).thenReturn(true);

    final String e164 = "+18005550123";
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, true, null, null);

    final Account account = createAccount(e164, attributes);

    verify(accounts).create(
        argThat(a -> e164.equals(a.getNumber().get()) && recentlyDeletedUuid.equals(a.getAccountIdentifier())),
        any());

    verify(keysManager).buildWriteItemsForNewDevice(eq(account.getAccountIdentifier()),
        eq(account.getPhoneNumberIdentifier()),
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
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null, discoverable, null, null);
    final Account account = createAccount("+18005550123", attributes);

    assertEquals(discoverable, account.isDiscoverableByPhoneNumber());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateWithStorageCapability(final boolean hasStorage) throws InterruptedException {
    final AccountAttributes attributes = new AccountAttributes(false, 1, 2, null, null,
            true, hasStorage ? Set.of(DeviceCapability.STORAGE) : Set.of(), null);

    final Account account = createAccount("+18005550123", attributes);

    assertEquals(hasStorage, account.hasCapability(DeviceCapability.STORAGE));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testAddDevice(boolean accountHasPhoneNumber) throws LinkDeviceTokenAlreadyUsedException {
    final String phoneNumber =
        PhoneNumberUtil.getInstance().format(PhoneNumberUtil.getInstance().getExampleNumber("US"),
            PhoneNumberUtil.PhoneNumberFormat.E164);

    final Account account = accountHasPhoneNumber
      ? AccountsHelper.generateTestAccount(phoneNumber, List.of(generateTestDevice(CLOCK.millis())))
      : AccountsHelper.generateTestAccountNoPhoneNumber(List.of(generateTestDevice(CLOCK.millis())));
    final UUID aci = account.getAccountIdentifier();
    final Optional<UUID> maybePni = account.getPhoneNumberIdentifier();
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
    when(accounts.getByAccountIdentifier(aci)).thenReturn(Optional.of(account));

    CLOCK.pin(CLOCK.instant().plusSeconds(60));

    final Pair<Account, Device> updatedAccountAndDevice = accountsManager.addDevice(
        aci,
        new DeviceSpec(
            deviceNameCiphertext,
            password,
            signalAgent,
            deviceCapabilities,
            new DeviceIdentityInfo(aciRegistrationId, aciSignedPreKey, aciPqLastResortPreKey),
            Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)).filter(_ -> accountHasPhoneNumber),
            true,
            Optional.empty(),
            Optional.empty()),
            accountsManager.generateLinkDeviceToken(aci));

    verify(keysManager).deleteSingleUsePreKeys(aci, nextDeviceId);
    maybePni.ifPresent(pni -> verify(keysManager).deleteSingleUsePreKeys(pni, nextDeviceId));
    verify(messagesManager).clear(aci, nextDeviceId);

    verify(keysManager).buildWriteItemsForNewDevice(
        aci,
        maybePni,
        nextDeviceId,
        aciSignedPreKey,
        maybePni.map(_ -> pniSignedPreKey),
        aciPqLastResortPreKey,
        maybePni.map(_ ->pniPqLastResortPreKey));

    verifyNoMoreInteractions(keysManager);
    final Device device = updatedAccountAndDevice.second();

    assertEquals(deviceNameCiphertext, device.getName());
    assertTrue(device.getAuthTokenHash().verify(password));
    assertEquals(signalAgent, device.getUserAgent());
    assertEquals(Collections.emptySet(), device.getCapabilities());
    assertEquals(aciRegistrationId, device.getAccountRegistrationId());
    assertEquals(accountHasPhoneNumber ? Optional.of(pniRegistrationId) : Optional.empty(), device.getPhoneNumberIdentityRegistrationId());
    assertTrue(device.getFetchesMessages());
    assertNull(device.getApnId());
    assertNull(device.getGcmId());
  }

  @ParameterizedTest
  @MethodSource
  void testUpdateDeviceLastSeen(final boolean expectUpdate, final long initialLastSeen, final long updatedLastSeen) {
    final Account account = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);

    final Device device = generateTestDevice(initialLastSeen);
    account.addDevice(device);

    accountsManager.updateDeviceLastSeen(account.getAccountIdentifier(), device, updatedLastSeen);

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
    addRetrievableAccount(account);
    account = accountsManager.changeNumber(uuid,
        targetNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, ecSignedPreKey),
        Map.of(Device.PRIMARY_ID, kemLastResortPreKey),
        Map.of(Device.PRIMARY_ID, 1));

    assertEquals(Optional.of(targetNumber), account.getNumber());

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

    addRetrievableAccount(account);

    phoneNumberIdentifiersByE164.put(originalNumber, account.getPhoneNumberIdentifier().orElseThrow());
    phoneNumberIdentifiersByE164.put(newNumber, account.getPhoneNumberIdentifier().orElseThrow());
    account = accountsManager.changeNumber(account.getAccountIdentifier(),
        newNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 1));

    assertEquals(Optional.of(newNumber), account.getNumber());
    assertEquals(Optional.of(phoneNumberIdentifier), account.getPhoneNumberIdentifier());
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
    addRetrievableAccount(account);

    account = accountsManager.changeNumber(uuid,
        targetNumber,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Map.of(Device.PRIMARY_ID, ecSignedPreKey),
        Map.of(Device.PRIMARY_ID, kemLastResoryPreKey),
        Map.of(Device.PRIMARY_ID, 1));

    assertEquals(Optional.of(targetNumber), account.getNumber());

    assertTrue(phoneNumberIdentifiersByE164.containsKey(targetNumber));
    final UUID newPni = phoneNumberIdentifiersByE164.get(targetNumber);

    verify(keysManager).deleteSingleUsePreKeys(existingAccountUuid);
    verify(keysManager).deleteSingleUsePreKeys(originalPni);
    verify(keysManager, atLeastOnce()).deleteSingleUsePreKeys(targetPni);
    verify(keysManager).deleteSingleUsePreKeys(newPni);
    verify(keysManager).buildWriteItemsForRemovedDevice(existingAccountUuid, Optional.of(targetPni), Device.PRIMARY_ID);
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
    addRetrievableAccount(account);
    final Account updatedAccount = accountsManager.changeNumber(
        uuid, targetNumber, new IdentityKey(ECKeyPair.generate().getPublicKey()), newSignedKeys, newSignedPqKeys, newRegistrationIds);

    assertEquals(Optional.of(targetNumber), updatedAccount.getNumber());

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
    addRetrievableAccount(account);
    assertThrows(MismatchedDevicesException.class,
        () -> accountsManager.changeNumber(
            uuid, targetNumber, new IdentityKey(ECKeyPair.generate().getPublicKey()), newSignedKeys, newSignedPqKeys, newRegistrationIds));

    verify(accounts, never()).changeNumber(any(), any(), any(), any(), any());
    verifyNoInteractions(keysManager);
  }

  @Test
  void testChangePhoneNumberViaUpdate() {
    final String originalNumber = "+14152222222";
    final String targetNumber = "+14153333333";
    final UUID uuid = UUID.randomUUID();

    final Account account = AccountsHelper.generateTestAccount(originalNumber, uuid, UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);

    assertThrows(AssertionError.class, () -> accountsManager.update(uuid, a -> a.setNumber(targetNumber, UUID.randomUUID())));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testReserveUsernameHash(boolean hasNumber) throws UsernameHashNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount(hasNumber ? "+18005551234" : null, new ArrayList<>());
    when(accounts.getByAccountIdentifier(account.getAccountIdentifier())).thenReturn(Optional.of(account));

    final List<byte[]> usernameHashes = List.of(TestRandomUtil.nextBytes(32), TestRandomUtil.nextBytes(32));

    final UsernameReservation result = accountsManager.reserveUsernameHash(account.getAccountIdentifier(), usernameHashes);
    assertArrayEquals(usernameHashes.getFirst(), result.reservedUsernameHash());
    verify(accounts, times(1)).reserveUsernameHash(eq(account), any(), eq(Duration.ofMinutes(5)));
  }

  @Test
  void testReserveOwnUsernameHash() throws UsernameHashNotAvailableException {
    final byte[] oldUsernameHash = TestRandomUtil.nextBytes(32);
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    account.setUsernameHash(oldUsernameHash);
    when(accounts.getByAccountIdentifier(account.getAccountIdentifier())).thenReturn(Optional.of(account));

    final List<byte[]> usernameHashes = List.of(TestRandomUtil.nextBytes(32), oldUsernameHash, TestRandomUtil.nextBytes(32));

    final UsernameReservation result = accountsManager.reserveUsernameHash(account.getAccountIdentifier(), usernameHashes);
    assertArrayEquals(oldUsernameHash, result.reservedUsernameHash());
    verify(accounts, never()).reserveUsernameHash(any(), any(), any());
  }

  @Test
  void testReserveUsernameOptimisticLockingFailure() throws UsernameHashNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    when(accounts.getByAccountIdentifier(account.getAccountIdentifier())).thenReturn(Optional.of(account));

    final List<byte[]> usernameHashes = List.of(TestRandomUtil.nextBytes(32), TestRandomUtil.nextBytes(32));

    doThrow(new ContestedOptimisticLockException())
        .doNothing()
        .when(accounts).reserveUsernameHash(any(), any(), any());

    final UsernameReservation result = accountsManager.reserveUsernameHash(account.getAccountIdentifier(), usernameHashes);
    assertArrayEquals(usernameHashes.getFirst(), result.reservedUsernameHash());
    verify(accounts, times(2)).reserveUsernameHash(eq(account), any(), eq(Duration.ofMinutes(5)));
  }

  @Test
  void testReserveUsernameHashAsyncNotAvailable() throws UsernameHashNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);

    doThrow(new UsernameHashNotAvailableException())
        .when(accounts).reserveUsernameHash(any(), any(), any());

    assertThrows(UsernameHashNotAvailableException.class, () ->
        accountsManager.reserveUsernameHash(account.getAccountIdentifier(), List.of(USERNAME_HASH_1, USERNAME_HASH_2)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConfirmReservedUsernameHash(final boolean hasNumber) throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount(hasNumber ? "+18005551234" : null, new ArrayList<>());
    addRetrievableAccount(account);

    setReservationHash(account, USERNAME_HASH_1);

    accountsManager.confirmReservedUsernameHash(account.getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    verify(accounts).confirmUsernameHash(eq(account), eq(USERNAME_HASH_1), eq(ENCRYPTED_USERNAME_1));
  }

  @Test
  void testConfirmReservedUsernameHashOptimisticLockingFailure() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    setReservationHash(account, USERNAME_HASH_1);
    when(accounts.getByAccountIdentifier(account.getAccountIdentifier())).thenReturn(Optional.of(account));

    doThrow(new ContestedOptimisticLockException())
        .doNothing()
        .when(accounts).confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    accountsManager.confirmReservedUsernameHash(account.getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    verify(accounts, times(2)).confirmUsernameHash(eq(account), eq(USERNAME_HASH_1), eq(ENCRYPTED_USERNAME_1));
  }

  @Test
  void testConfirmReservedHashNameMismatch() {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);

    setReservationHash(account, USERNAME_HASH_1);
    assertThrows(UsernameReservationNotFoundException.class,
        () -> accountsManager.confirmReservedUsernameHash(account.getAccountIdentifier(), USERNAME_HASH_2, ENCRYPTED_USERNAME_2));
  }

  @Test
  void testConfirmReservedLapsed() throws UsernameHashNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);
    // hash was reserved, but the reservation lapsed and another account took it
    setReservationHash(account, USERNAME_HASH_1);
    doThrow(new UsernameHashNotAvailableException())
        .when(accounts).confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accountsManager.confirmReservedUsernameHash(account.getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertTrue(account.getUsernameHash().isEmpty());
  }

  @Test
  void testConfirmReservedRetry() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);
    account.setUsernameHash(USERNAME_HASH_1);

    // reserved username already set, should be treated as a replay
    accountsManager.confirmReservedUsernameHash(account.getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    verify(accounts, never()).confirmUsernameHash(any(), any(), any());
  }

  @Test
  void testConfirmReservedUsernameHashWithNoReservation() throws UsernameHashNotAvailableException {
    final Account account = AccountsHelper.generateTestAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID(),
        new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(account);

    assertThrows(UsernameReservationNotFoundException.class,
        () -> accountsManager.confirmReservedUsernameHash(account.getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    verify(accounts, never()).confirmUsernameHash(any(), any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testClearUsernameHash(final boolean hasNumber) {
    final Account account = AccountsHelper.generateTestAccount(hasNumber ? "+18005551234" : null, new ArrayList<>());
    addRetrievableAccount(account);

    account.setUsernameHash(USERNAME_HASH_1);
    accountsManager.clearUsernameHash(account.getAccountIdentifier());
    verify(accounts).clearUsernameHash(eq(account));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSetUsernameViaUpdate(final boolean hasNumber) {
    final Account account = AccountsHelper.generateTestAccount(hasNumber ? "+18005551234" : null, new ArrayList<>());
    addRetrievableAccount(account);

    assertThrows(AssertionError.class, () ->
        accountsManager.update(account.getAccountIdentifier(), a -> a.setUsernameHash(USERNAME_HASH_1)));
  }

  @Test
  void testOnlyPrimaryCanWaitForDeviceLinked() {
    final Device primaryDevice = new Device();
    primaryDevice.setId(Device.PRIMARY_ID);

    final Device linkedDevice = new Device();
    linkedDevice.setId((byte) (Device.PRIMARY_ID + 1));

    final Account account = AccountsHelper.generateTestAccount("+14152222222", List.of(primaryDevice, linkedDevice));

    assertThrows(IllegalArgumentException.class,
        () -> accountsManager.waitForNewLinkedDevice(account.getAccountIdentifier(), linkedDevice, "", Duration.ofSeconds(1)));

  }

  @ParameterizedTest
  @ValueSource(strings = {
      "AccountsManagerTest-testJsonRoundTripSerialization.json",
      "AccountsManagerTest-testJsonRoundTripSerializationNumberless.json"})
  void testJsonRoundTripSerialization(final String fileName) throws Exception {
    String originalJson;
    try (InputStream inputStream = getClass().getResourceAsStream(fileName)) {
      Objects.requireNonNull(inputStream);
      originalJson = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    final Account originalAccount = AccountsManager.parseAccountJson(originalJson,
        UUID.fromString("111111-1111-1111-1111-111111111111")).orElseThrow();

    final String serialized = AccountsManager.writeRedisAccountJson(originalAccount);
    final Account parsedAccount = AccountsManager.parseAccountJson(serialized, originalAccount.getAccountIdentifier()).orElseThrow();

    assertEquals(originalAccount.getAccountIdentifier(), parsedAccount.getAccountIdentifier());
    assertEquals(originalAccount.getPhoneNumberIdentifier(), parsedAccount.getPhoneNumberIdentifier());
    assertEquals(originalAccount.getNumber(), parsedAccount.getNumber());
    assertArrayEquals(originalAccount.getUnidentifiedAccessKey().orElseThrow(),
        parsedAccount.getUnidentifiedAccessKey().orElseThrow());
    assertEquals(originalAccount.isDiscoverableByPhoneNumber(), parsedAccount.isDiscoverableByPhoneNumber());
    assertEquals(originalAccount.isUnrestrictedUnidentifiedAccess(), parsedAccount.isUnrestrictedUnidentifiedAccess());

    assertEquals(originalAccount.getDevices().size(), parsedAccount.getDevices().size());

    final Device originalDevice = originalAccount.getPrimaryDevice();
    final Device parsedDevice = parsedAccount.getPrimaryDevice();

    assertEquals(originalDevice.getId(), parsedDevice.getId());
    assertEquals(originalDevice.getAccountRegistrationId(), parsedDevice.getAccountRegistrationId());
    assertEquals(originalDevice.getPhoneNumberIdentityRegistrationId(), parsedDevice.getPhoneNumberIdentityRegistrationId());
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

  private Account createAccount(final AccountAttributes accountAttributes) throws ReceiptAlreadyRedeemedException {
    final ECKeyPair aciKeyPair = ECKeyPair.generate();

    return accountsManager.create(accountAttributes,
        new IdentityKey(aciKeyPair.getPublicKey()),
        mock(ReceiptCredentialPresentation.class),
        new DeviceSpec(
            accountAttributes.getName(),
            "password",
            null,
            accountAttributes.getCapabilities(),
            new DeviceIdentityInfo(accountAttributes.getRegistrationId(), KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
            Optional.empty(),
            accountAttributes.getFetchesMessages(),
            Optional.empty(),
            Optional.empty()),
        null);
  }

  private Account createAccount(final String e164, final AccountAttributes accountAttributes) {
    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    return accountsManager.create(e164,
        accountAttributes,
        new IdentityKey(aciKeyPair.getPublicKey()),
        new IdentityKey(pniKeyPair.getPublicKey()),
        new DeviceSpec(
            accountAttributes.getName(),
            "password",
            null,
            accountAttributes.getCapabilities(),
            new DeviceIdentityInfo(accountAttributes.getRegistrationId(), KeysHelper.signedECPreKey(1, aciKeyPair), KeysHelper.signedKEMPreKey(3, aciKeyPair)),
            Optional.of(new DeviceIdentityInfo(accountAttributes.getPhoneNumberIdentityRegistrationId().orElseThrow(() -> new AssertionError("PNI registration ID must be provided for an account with a phone number")),
                KeysHelper.signedECPreKey(2, pniKeyPair),
                KeysHelper.signedKEMPreKey(4, pniKeyPair))),
            accountAttributes.getFetchesMessages(),
            Optional.empty(),
            Optional.empty()),
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

  @ParameterizedTest
  @MethodSource
  void updateCurrentProfileVersion(final byte[] currentVersion, final byte[] expectedVersion, final byte[] newVersion, final boolean expectException) throws Exception {
    final Account accountWithNumber = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    addRetrievableAccount(accountWithNumber);

    final Account accountWithoutNumber = AccountsHelper.generateTestAccountNoPhoneNumber(new ArrayList<>());
    addRetrievableAccount(accountWithoutNumber);

    for (final Account account : List.of(accountWithNumber, accountWithoutNumber)) {
      account.setCurrentProfileVersion(currentVersion);

      final AccountBadge badge = new AccountBadge("test", CLOCK.instant().plusSeconds(60), true);

      assertTrue(account.getBadges().isEmpty());

      if (expectException) {
        assertThrows(WriteConflictException.class, () -> accountsManager.updateCurrentProfileVersion(account.getAccountIdentifier(), newVersion, expectedVersion, _ -> {}));
      } else {
        final Account updatedAccount = accountsManager.updateCurrentProfileVersion(account.getAccountIdentifier(), newVersion,
            expectedVersion, a -> {

              a.setBadges(CLOCK, new ArrayList<>(List.of(badge)));
            });

        assertArrayEquals(newVersion, updatedAccount.getCurrentProfileVersion().orElseThrow());
        assertEquals(List.of(badge), updatedAccount.getBadges());
      }
    }
  }

  static Collection<Arguments> updateCurrentProfileVersion() {

    final byte[] empty = new byte[0];
    final byte[] version1 = TestRandomUtil.nextBytes(16);
    final byte[] version2 = Arrays.copyOf(version1, version1.length);
    version2[0] = (byte) (version2[0] + 1);

    return List.of(
        Arguments.argumentSet("no current version - matches", empty, empty, version1, false),
        Arguments.argumentSet("no current version - conflict", empty, version1, version1, true),
        Arguments.argumentSet("current version - empty conflict", version1, empty, version2, true),
        Arguments.argumentSet("current version - matches", version1, version1, version2, false)
    );
  }


  @Test
  void getAccountsForChangeNumber() {
    final Account account = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), new ArrayList<>(), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    final UUID accountIdentifier = account.getAccountIdentifier();
    addRetrievableAccount(account);

    final String targetNumber = "+13102222222";

    assertFalse(phoneNumberIdentifiersByE164.containsKey(targetNumber));

    final Pair<Account, Optional<Account>> accountsForChangeNumber = accountsManager.getAccountsForChangeNumber(
        accountIdentifier, targetNumber);

    assertEquals(account, accountsForChangeNumber.first());
    verify(accounts).getByAccountIdentifier(accountIdentifier);
    // getPhoneNumberIdentifier handles alternate forms
    verify(phoneNumberIdentifiers).getPhoneNumberIdentifier(targetNumber);
  }

  @Test
  void createAccountWithoutNumberOrRecoveryPassword() {
    assertThrows(IllegalArgumentException.class,
        () -> accountsManager.create(new AccountAttributes(),
            new IdentityKey(ECKeyPair.generate().getPublicKey()),
            ReceiptCredentialTestUtil.receiptPresentation(),
            mock(DeviceSpec.class),
            null));
  }

  private void addRetrievableAccount(final Account account) {
    when(accounts.getByAccountIdentifier(account.getAccountIdentifier()))
        .thenReturn(Optional.of(account));

    when(accounts.getByAccountIdentifierAsync(account.getAccountIdentifier()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
  }

  @Nested
  class Totp {

    @Test
    void generatePendingTotpKey() throws TooManyTotpKeysException {
      final UUID accountIdentifier = UUID.randomUUID();

      final Account account = mock(Account.class);
      when(account.getAccountIdentifier()).thenReturn(accountIdentifier);

      when(accounts.getByAccountIdentifier(accountIdentifier))
          .thenReturn(Optional.of(account));

      final TotpKey pendingTotpKey = accountsManager.generatePendingTotpKey(accountIdentifier);

      verify(account).setPendingTotpKey(pendingTotpKey);
    }

    @Test
    void generatePendingTotpKeyTooManyConfirmedKeys() {
      final UUID accountIdentifier = UUID.randomUUID();

      final Account account = mock(Account.class);
      when(account.getAccountIdentifier()).thenReturn(accountIdentifier);

      when(account.getTotpKeys()).thenReturn(IntStream.range(0, AccountsManager.MAX_TOTP_KEYS)
          .boxed()
          .collect(Collectors.toMap(Integer::byteValue, _ -> new AnnotatedTotpKey(
              new TotpKey(AccountsManager.TOTP_PARAMETERS, TestRandomUtil.nextBytes(16)),
              TestRandomUtil.nextBytes(16)))));

      when(accounts.getByAccountIdentifier(accountIdentifier))
          .thenReturn(Optional.of(account));

      assertThrows(TooManyTotpKeysException.class, () -> accountsManager.generatePendingTotpKey(accountIdentifier));
      verify(account, never()).setPendingTotpKey(any());
    }

    @Test
    void confirmPendingTotpKey() throws InvalidKeyException, TooManyTotpKeysException, NoSuchAlgorithmException {
      final UUID accountIdentifier = UUID.randomUUID();

      final Account account = mock(Account.class);
      when(account.getAccountIdentifier()).thenReturn(accountIdentifier);

      when(accounts.getByAccountIdentifier(accountIdentifier))
          .thenReturn(Optional.of(account));

      final TotpKey pendingTotpKey = accountsManager.generatePendingTotpKey(accountIdentifier);
      final byte nextTotpKeyId = (byte) ThreadLocalRandom.current().nextInt();

      when(account.getPendingTotpKey()).thenReturn(Optional.of(pendingTotpKey));
      when(account.getNextTotpKeyId()).thenReturn(nextTotpKeyId);

      final TimeBasedOneTimePasswordGenerator totpGenerator =
          new TimeBasedOneTimePasswordGenerator(AccountsManager.TOTP_PARAMETERS.timeStep(),
              AccountsManager.TOTP_PARAMETERS.passwordLength(),
              AccountsManager.TOTP_PARAMETERS.algorithm());

      final Instant timestamp = Instant.now();

      assertEquals(Optional.of(nextTotpKeyId), accountsManager.confirmPendingTotpKey(accountIdentifier,
          totpGenerator.generateOneTimePassword(pendingTotpKey, timestamp),
          timestamp,
          TestRandomUtil.nextBytes(16)));

      verify(account).setPendingTotpKey(null);
    }

    @Test
    void confirmPendingTotpKeyPreviouslyConfirmed() throws InvalidKeyException, NoSuchAlgorithmException {
      final UUID accountIdentifier = UUID.randomUUID();

      final Account account = mock(Account.class);
      when(account.getAccountIdentifier()).thenReturn(accountIdentifier);

      when(accounts.getByAccountIdentifier(accountIdentifier))
          .thenReturn(Optional.of(account));

      final AnnotatedTotpKey confirmedTotpKey;
      {
        final KeyGenerator totpKeyGenerator = KeyGenerator.getInstance(AccountsManager.TOTP_PARAMETERS.algorithm());
        totpKeyGenerator.init(AccountsManager.TOTP_KEY_LENGTH_BITS);

        confirmedTotpKey = new AnnotatedTotpKey(
            new TotpKey(AccountsManager.TOTP_PARAMETERS, totpKeyGenerator.generateKey().getEncoded()),
            TestRandomUtil.nextBytes(16));
      }

      final byte keyId = 17;

      when(account.getPendingTotpKey()).thenReturn(Optional.empty());
      when(account.getTotpKeys()).thenReturn(Map.of(
          (byte) (keyId - 1), confirmedTotpKey,
          keyId, confirmedTotpKey));

      final TimeBasedOneTimePasswordGenerator totpGenerator =
          new TimeBasedOneTimePasswordGenerator(AccountsManager.TOTP_PARAMETERS.timeStep(),
              AccountsManager.TOTP_PARAMETERS.passwordLength(),
              AccountsManager.TOTP_PARAMETERS.algorithm());

      final Instant timestamp = Instant.now();

      assertEquals(Optional.of(keyId), accountsManager.confirmPendingTotpKey(accountIdentifier,
          totpGenerator.generateOneTimePassword(confirmedTotpKey, timestamp),
          timestamp,
          TestRandomUtil.nextBytes(16)));
    }

    @Test
    void confirmPendingTotpKeyNoKeys() throws InvalidKeyException, TooManyTotpKeysException, NoSuchAlgorithmException {
      final UUID accountIdentifier = UUID.randomUUID();

      final Account account = mock(Account.class);
      when(account.getAccountIdentifier()).thenReturn(accountIdentifier);

      when(accounts.getByAccountIdentifier(accountIdentifier))
          .thenReturn(Optional.of(account));

      final TotpKey pendingTotpKey = accountsManager.generatePendingTotpKey(accountIdentifier);

      when(account.getPendingTotpKey()).thenReturn(Optional.empty());
      when(account.getTotpKeys()).thenReturn(Collections.emptyMap());

      final TimeBasedOneTimePasswordGenerator totpGenerator =
          new TimeBasedOneTimePasswordGenerator(AccountsManager.TOTP_PARAMETERS.timeStep(),
              AccountsManager.TOTP_PARAMETERS.passwordLength(),
              AccountsManager.TOTP_PARAMETERS.algorithm());

      final Instant timestamp = Instant.now();

      assertEquals(Optional.empty(), accountsManager.confirmPendingTotpKey(accountIdentifier,
          totpGenerator.generateOneTimePassword(pendingTotpKey, timestamp),
          timestamp,
          TestRandomUtil.nextBytes(16)));
    }

    @Test
    void confirmPendingTotpKeyIncorrectPassword()
        throws InvalidKeyException, TooManyTotpKeysException, NoSuchAlgorithmException {
      final UUID accountIdentifier = UUID.randomUUID();

      final Account account = mock(Account.class);
      when(account.getAccountIdentifier()).thenReturn(accountIdentifier);

      when(accounts.getByAccountIdentifier(accountIdentifier))
          .thenReturn(Optional.of(account));

      final TotpKey pendingTotpKey = accountsManager.generatePendingTotpKey(accountIdentifier);
      final byte nextTotpKeyId = (byte) ThreadLocalRandom.current().nextInt();

      when(account.getPendingTotpKey()).thenReturn(Optional.of(pendingTotpKey));
      when(account.getNextTotpKeyId()).thenReturn(nextTotpKeyId);

      final TimeBasedOneTimePasswordGenerator totpGenerator =
          new TimeBasedOneTimePasswordGenerator(AccountsManager.TOTP_PARAMETERS.timeStep(),
              AccountsManager.TOTP_PARAMETERS.passwordLength(),
              AccountsManager.TOTP_PARAMETERS.algorithm());

      final Instant timestamp = Instant.now();
      final int incorrectPassword = totpGenerator.generateOneTimePassword(pendingTotpKey, timestamp) + 1;

      assertEquals(Optional.empty(), accountsManager.confirmPendingTotpKey(accountIdentifier,
          incorrectPassword,
          timestamp,
          TestRandomUtil.nextBytes(16)));

      verify(account, never()).setPendingTotpKey(null);
    }
  }

  @ParameterizedTest
  @MethodSource
  void verifyTotp(final Map<Byte, AnnotatedTotpKey> totpKeys,
      final Instant timestamp,
      @Nullable final Integer oneTimePassword,
      final boolean expectVerified) {

    final Account account = mock(Account.class);
    when(account.getTotpKeys()).thenReturn(totpKeys);

    assertEquals(expectVerified, accountsManager.verifyTotp(account, timestamp, oneTimePassword));
  }

  private static List<Arguments> verifyTotp() throws NoSuchAlgorithmException, InvalidKeyException {
    final Instant timestamp = Instant.now();

    final AnnotatedTotpKey totpKey;
    final AnnotatedTotpKey secondTotpKey;
    {
      final KeyGenerator totpKeyGenerator = KeyGenerator.getInstance(AccountsManager.TOTP_PARAMETERS.algorithm());
      totpKeyGenerator.init(AccountsManager.TOTP_KEY_LENGTH_BITS);

      totpKey = new AnnotatedTotpKey(
          new TotpKey(AccountsManager.TOTP_PARAMETERS, totpKeyGenerator.generateKey().getEncoded()),
          TestRandomUtil.nextBytes(16));

      secondTotpKey = new AnnotatedTotpKey(
          new TotpKey(AccountsManager.TOTP_PARAMETERS, totpKeyGenerator.generateKey().getEncoded()),
          TestRandomUtil.nextBytes(16));
    }

    final TimeBasedOneTimePasswordGenerator totpGenerator =
        new TimeBasedOneTimePasswordGenerator(AccountsManager.TOTP_PARAMETERS.timeStep(),
            AccountsManager.TOTP_PARAMETERS.passwordLength(),
            AccountsManager.TOTP_PARAMETERS.algorithm());

    return List.of(
        Arguments.argumentSet("No keys, no password provided",
            Collections.emptyMap(), timestamp, null, true),

        Arguments.argumentSet("No keys, password provided",
            Collections.emptyMap(), timestamp, 123456, false),

        Arguments.argumentSet("Has key, correct password provided",
            Map.of((byte) 1, totpKey), timestamp, totpGenerator.generateOneTimePassword(totpKey, timestamp), true),

        Arguments.argumentSet("Has key, incorrect password provided",
            Map.of((byte) 1, totpKey), timestamp, totpGenerator.generateOneTimePassword(totpKey, timestamp) + 1, false),

        Arguments.argumentSet("Has key, no password provided",
            Map.of((byte) 1, totpKey), timestamp, null, false),

        Arguments.argumentSet("Has multiple keys, correct password provided for one key",
            Map.of((byte) 1, totpKey, (byte) 2, secondTotpKey), timestamp, totpGenerator.generateOneTimePassword(totpKey, timestamp), true)
    );
  }

  @RepeatedTest(value = 10, failureThreshold = 2)
  void verifyTotpWithDelay() throws NoSuchAlgorithmException, InvalidKeyException {
    final AnnotatedTotpKey totpKey;
    {
      final KeyGenerator totpKeyGenerator = KeyGenerator.getInstance(AccountsManager.TOTP_PARAMETERS.algorithm());
      totpKeyGenerator.init(AccountsManager.TOTP_KEY_LENGTH_BITS);

      totpKey = new AnnotatedTotpKey(
          new TotpKey(AccountsManager.TOTP_PARAMETERS, totpKeyGenerator.generateKey().getEncoded()),
          TestRandomUtil.nextBytes(16));
    }

    final TimeBasedOneTimePasswordGenerator totpGenerator =
        new TimeBasedOneTimePasswordGenerator(AccountsManager.TOTP_PARAMETERS.timeStep(),
            AccountsManager.TOTP_PARAMETERS.passwordLength(),
            AccountsManager.TOTP_PARAMETERS.algorithm());

    final Account account = mock(Account.class);
    when(account.getTotpKeys()).thenReturn(Map.of((byte) 1, totpKey));

    final Instant beginningOfTotpWindow =
        Instant.ofEpochMilli((Instant.now().toEpochMilli() / AccountsManager.TOTP_PARAMETERS.timeStep().toMillis()) *
            AccountsManager.TOTP_PARAMETERS.timeStep().toMillis());

    final int oneTimePassword = totpGenerator.generateOneTimePassword(totpKey, beginningOfTotpWindow);

    assertTrue(accountsManager.verifyTotp(account, beginningOfTotpWindow, oneTimePassword),
        "One-time password should be valid at the start of the window in which it was generated");

    assertTrue(accountsManager.verifyTotp(account, beginningOfTotpWindow.plus(AccountsManager.TOTP_PARAMETERS.timeStep()), oneTimePassword),
        "One-time password should be valid at the start of the window after which it was generated");

    assertTrue(accountsManager.verifyTotp(account, beginningOfTotpWindow.plus(AccountsManager.TOTP_PARAMETERS.timeStep()).plus(MAX_TOTP_VALIDATION_DELAY).minusMillis(1), oneTimePassword),
        "One-time password should be valid up until max delay after end of current TOTP window");

    // With six-digit OTPs, there's a one-in-a-million chance of this returning a false positive, and so we repeat the
    // test several allowing for failure
    assertFalse(accountsManager.verifyTotp(account, beginningOfTotpWindow.plus(AccountsManager.TOTP_PARAMETERS.timeStep()).plus(MAX_TOTP_VALIDATION_DELAY), oneTimePassword),
        "One-time password should not be valid after max delay past end of current TOTP window");
  }
}
