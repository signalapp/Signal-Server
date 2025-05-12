/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.JsonHelpers;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.Pair;


class AccountsManagerConcurrentModificationIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.ACCOUNTS,
      Tables.NUMBERS,
      Tables.PNI_ASSIGNMENTS,
      Tables.DELETED_ACCOUNTS,
      Tables.EC_KEYS,
      Tables.PQ_KEYS,
      Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS);

  private Accounts accounts;

  private AccountsManager accountsManager;

  private RedisAdvancedClusterCommands<String, String> commands;

  private Executor mutationExecutor = new ThreadPoolExecutor(20, 20, 5, TimeUnit.SECONDS, new LinkedBlockingDeque<>(20));

  @BeforeEach
  void setup() throws Exception {

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    accounts = new Accounts(
        Clock.systemUTC(),
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName());

    {
      //noinspection unchecked
      commands = mock(RedisAdvancedClusterCommands.class);

      final AccountLockManager accountLockManager = mock(AccountLockManager.class);

      doAnswer(invocation -> {
        final Callable<?> task = invocation.getArgument(1);
        return task.call();
      }).when(accountLockManager).withLock(anyList(), any(), any());

      when(accountLockManager.withLockAsync(anyList(), any(), any())).thenAnswer(invocation -> {
        final Supplier<CompletableFuture<?>> taskSupplier = invocation.getArgument(1);
        taskSupplier.get().join();

        return CompletableFuture.completedFuture(null);
      });

      final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
      when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString()))
          .thenAnswer((Answer<CompletableFuture<UUID>>) invocation -> CompletableFuture.completedFuture(UUID.randomUUID()));

      accountsManager = new AccountsManager(
          accounts,
          phoneNumberIdentifiers,
          RedisClusterHelper.builder().stringCommands(commands).build(),
          mock(FaultTolerantRedisClient.class),
          accountLockManager,
          mock(KeysManager.class),
          mock(MessagesManager.class),
          mock(ProfilesManager.class),
          mock(SecureStorageClient.class),
          mock(SecureValueRecovery2Client.class),
          mock(DisconnectionRequestManager.class),
          mock(RegistrationRecoveryPasswordsManager.class),
          mock(ClientPublicKeysManager.class),
          mock(Executor.class),
          mock(ScheduledExecutorService.class),
          mock(Clock.class),
          "link-device-secret".getBytes(StandardCharsets.UTF_8),
          dynamicConfigurationManager
      );
    }
  }

  @Test
  void testConcurrentUpdate() throws IOException, InterruptedException {
    final UUID uuid;
    {
      final ECKeyPair aciKeyPair = Curve.generateKeyPair();
      final ECKeyPair pniKeyPair = Curve.generateKeyPair();

      final Account account = accountsManager.update(
          accountsManager.create("+14155551212",
              new AccountAttributes(),
              new ArrayList<>(),
              new IdentityKey(aciKeyPair.getPublicKey()),
              new IdentityKey(pniKeyPair.getPublicKey()),
              new DeviceSpec(
                  null,
                  "password",
                  null,
                  Set.of(),
                  1,
                  2,
                  true,
                  Optional.empty(),
                  Optional.empty(),
                  KeysHelper.signedECPreKey(1, aciKeyPair),
                  KeysHelper.signedECPreKey(2, pniKeyPair),
                  KeysHelper.signedKEMPreKey(3, aciKeyPair),
                  KeysHelper.signedKEMPreKey(4, pniKeyPair)),
              null),
          a -> {
            a.setUnidentifiedAccessKey(new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
            a.removeDevice(Device.PRIMARY_ID);
            a.addDevice(DevicesHelper.createDevice(Device.PRIMARY_ID));
          });

      uuid = account.getUuid();
    }

    final boolean discoverableByPhoneNumber = false;
    final String currentProfileVersion = "cpv";
    final IdentityKey identityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());
    final byte[] unidentifiedAccessKey = new byte[]{1};
    final String pin = "1234";
    final String registrationLock = "reglock";
    final SaltedTokenHash credentials = SaltedTokenHash.generateFor(registrationLock);
    final boolean unrestrictedUnidentifiedAccess = true;
    final long lastSeen = Instant.now().getEpochSecond();

    CompletableFuture.allOf(
        modifyAccount(uuid, account -> account.setDiscoverableByPhoneNumber(discoverableByPhoneNumber)),
        modifyAccount(uuid, account -> account.setCurrentProfileVersion(currentProfileVersion)),
        modifyAccount(uuid, account -> account.setIdentityKey(identityKey)),
        modifyAccount(uuid, account -> account.setUnidentifiedAccessKey(unidentifiedAccessKey)),
        modifyAccount(uuid, account -> account.setRegistrationLock(credentials.hash(), credentials.salt())),
        modifyAccount(uuid, account -> account.setUnrestrictedUnidentifiedAccess(unrestrictedUnidentifiedAccess)),
        modifyDevice(uuid, Device.PRIMARY_ID, device -> device.setLastSeen(lastSeen)),
        modifyDevice(uuid, Device.PRIMARY_ID, device -> device.setName("deviceName".getBytes(StandardCharsets.UTF_8)))
    ).join();

    final Account managerAccount = accountsManager.getByAccountIdentifier(uuid).orElseThrow();
    final Account dynamoAccount = accounts.getByAccountIdentifier(uuid).orElseThrow();

    final Account redisAccount = getLastAccountFromRedisMock(commands);

    Stream.of(
        new Pair<>("manager", managerAccount),
        new Pair<>("dynamo", dynamoAccount),
        new Pair<>("redis", redisAccount)
    ).forEach(pair ->
        verifyAccount(pair.first(), pair.second(), discoverableByPhoneNumber,
            currentProfileVersion, identityKey, unidentifiedAccessKey, pin, registrationLock,
            unrestrictedUnidentifiedAccess, lastSeen));
  }

  private Account getLastAccountFromRedisMock(RedisAdvancedClusterCommands<String, String> commands) throws IOException {
    ArgumentCaptor<String> redisSetArgumentCapture = ArgumentCaptor.forClass(String.class);

    verify(commands, atLeast(20)).setex(anyString(), anyLong(), redisSetArgumentCapture.capture());

    return JsonHelpers.fromJson(redisSetArgumentCapture.getValue(), Account.class);
  }

  private void verifyAccount(final String name, final Account account, final boolean discoverableByPhoneNumber, final String currentProfileVersion, final IdentityKey identityKey, final byte[] unidentifiedAccessKey, final String pin, final String clientRegistrationLock, final boolean unrestrictedUnidentifiedAccess, final long lastSeen) {

    assertAll(name,
        () -> assertEquals(discoverableByPhoneNumber, account.isDiscoverableByPhoneNumber()),
        () -> assertEquals(currentProfileVersion, account.getCurrentProfileVersion().orElseThrow()),
        () -> assertEquals(identityKey, account.getIdentityKey(IdentityType.ACI)),
        () -> assertArrayEquals(unidentifiedAccessKey, account.getUnidentifiedAccessKey().orElseThrow()),
        () -> assertTrue(account.getRegistrationLock().verify(clientRegistrationLock)),
        () -> assertEquals(unrestrictedUnidentifiedAccess, account.isUnrestrictedUnidentifiedAccess())
    );
  }

  private CompletableFuture<?> modifyAccount(final UUID uuid, final Consumer<Account> accountMutation) {

    return CompletableFuture.runAsync(() -> {
      final Account account = accountsManager.getByAccountIdentifier(uuid).orElseThrow();
      accountsManager.update(account, accountMutation);
    }, mutationExecutor);
  }

  private CompletableFuture<?> modifyDevice(final UUID uuid, final byte deviceId, final Consumer<Device> deviceMutation) {

    return CompletableFuture.runAsync(() -> {
      final Account account = accountsManager.getByAccountIdentifier(uuid).orElseThrow();
      accountsManager.updateDevice(account, deviceId, deviceMutation);
    }, mutationExecutor);
  }
}
