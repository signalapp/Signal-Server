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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
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
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.JsonHelpers;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.ThrowingSupplier;


class AccountsManagerConcurrentModificationIntegrationTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.ACCOUNTS,
      Tables.NUMBERS,
      Tables.PNI_ASSIGNMENTS,
      Tables.DELETED_ACCOUNTS,
      Tables.EC_KEYS,
      Tables.PAGED_PQ_KEYS,
      Tables.REDEEMED_RECEIPTS,
      Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS,
      Tables.PHONE_NUMBER_RECOVERY_PASSWORDS);

  private Accounts accounts;

  private AccountsManager accountsManager;

  private RedisAdvancedClusterCommands<String, String> commands;

  private Executor mutationExecutor = new ThreadPoolExecutor(20, 20, 5, TimeUnit.SECONDS, new LinkedBlockingDeque<>(20));

  @BeforeEach
  void setup() throws Exception {

    accounts = new Accounts(
        Clock.systemUTC(),
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        new RedeemedReceiptsManager(Clock.systemUTC(), Tables.REDEEMED_RECEIPTS.tableName(),
            DYNAMO_DB_EXTENSION.getDynamoDbClient()),
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
        final ThrowingSupplier<?, ?> task = invocation.getArgument(1);
        return task.get();
      }).when(accountLockManager).withLock(anySet(), any(), any());

      final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
      when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString()))
          .thenAnswer((Answer<CompletableFuture<UUID>>) _ -> CompletableFuture.completedFuture(UUID.randomUUID()));

      final PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager =
          new PhoneNumberRecoveryPasswordsManager(new PhoneNumberRecoveryPasswords(
              Tables.PHONE_NUMBER_RECOVERY_PASSWORDS.tableName(),
              Duration.ofDays(1),
              DYNAMO_DB_EXTENSION.getDynamoDbClient(),
              Clock.systemUTC()));

      accountsManager = new AccountsManager(
          accounts,
          phoneNumberIdentifiers,
          RedisClusterHelper.builder().stringCommands(commands).build(),
          mock(FaultTolerantRedisClient.class),
          accountLockManager,
          mock(KeysManager.class),
          mock(MessagesManager.class),
          mock(ProfilesManager.class),
          mock(ChangeNumberWaitingPeriodManager.class),
          mock(SecureStorageClient.class),
          mock(SecureValueRecoveryClient.class),
          mock(DisconnectionRequestManager.class),
          phoneNumberRecoveryPasswordsManager,
          mock(Executor.class),
          mock(ScheduledExecutorService.class),
          mock(ScheduledExecutorService.class),
          mock(Clock.class),
          "link-device-secret".getBytes(StandardCharsets.UTF_8),
          AccountsManager.TOTP_PARAMETERS.timeStep().dividedBy(2)
      );
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testConcurrentUpdate(final boolean numberless) throws IOException {
    final UUID aci;
    {

      final AccountsHelper.AccountBuilder accountBuilder = new AccountsHelper.AccountBuilder(accountsManager);
      if (!numberless) {
        accountBuilder.e164("+14155551212");
      }

      aci = accountBuilder.build().getAccountIdentifier();

      // set some additional attributes
      accountsManager.update(aci,
          a -> {
            a.setUnidentifiedAccessKey(new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
            a.removeDevice(Device.PRIMARY_ID);
            a.addDevice(DevicesHelper.createDevice(Device.PRIMARY_ID));
          });
    }

    final byte[] currentProfileVersion = new byte[32];
    final IdentityKey identityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());
    final byte[] unidentifiedAccessKey = new byte[]{1};
    final String pin = "1234";
    final String registrationLock = "reglock";
    final SaltedTokenHash credentials = SaltedTokenHash.generateFor(registrationLock);
    final String accountRecovery = "account-recovery";
    final SaltedTokenHash accountRecoveryPassword = SaltedTokenHash.generateFor(accountRecovery);
    final boolean unrestrictedUnidentifiedAccess = true;
    final long lastSeen = Instant.now().getEpochSecond();

    CompletableFuture.allOf(
        modifyAccount(aci, account -> account.setDiscoverableByPhoneNumber(true)),
        modifyAccount(aci, account -> account.setCurrentProfileVersion(currentProfileVersion)),
        modifyAccount(aci, account -> account.setIdentityKey(identityKey)),
        modifyAccount(aci, account -> account.setUnidentifiedAccessKey(unidentifiedAccessKey)),
        modifyAccount(aci, account -> {
              if (!numberless) {
                account.setRegistrationLock(credentials.hash(), credentials.salt());
              }
            }),
        modifyAccount(aci, account -> account.setAccountRecoveryPassword(accountRecoveryPassword)),
        modifyAccount(aci, account -> account.setUnrestrictedUnidentifiedAccess(unrestrictedUnidentifiedAccess)),
        modifyDevice(aci, Device.PRIMARY_ID, device -> device.setLastSeen(lastSeen)),
        modifyDevice(aci, Device.PRIMARY_ID, device -> device.setName("deviceName".getBytes(StandardCharsets.UTF_8)))
    ).join();

    final Account managerAccount = accountsManager.getByAccountIdentifier(aci).orElseThrow();
    final Account dynamoAccount = accounts.getByAccountIdentifier(aci).orElseThrow();

    // accounts with numbers have twice as many setex calls, because they include a setex(pni, aci) call
    final Account redisAccount = getLastAccountFromRedisMock(commands, numberless ? 10 : 20);

    Stream.of(
        new Pair<>("manager", managerAccount),
        new Pair<>("dynamo", dynamoAccount),
        new Pair<>("redis", redisAccount)
    ).forEach(pair ->
        verifyAccount(pair.first(), pair.second(), !numberless,
            currentProfileVersion, identityKey, unidentifiedAccessKey, pin, registrationLock, accountRecovery,
            unrestrictedUnidentifiedAccess, lastSeen));
  }

  private Account getLastAccountFromRedisMock(RedisAdvancedClusterCommands<String, String> commands, final int minimumSetExCalls) throws IOException {
    ArgumentCaptor<String> redisSetArgumentCapture = ArgumentCaptor.forClass(String.class);


    verify(commands, atLeast(minimumSetExCalls)).setex(anyString(), anyLong(), redisSetArgumentCapture.capture());

    return JsonHelpers.fromJson(redisSetArgumentCapture.getValue(), Account.class);
  }

  private void verifyAccount(final String name, final Account account, final boolean discoverableByPhoneNumber, final byte[] currentProfileVersion, final IdentityKey identityKey, final byte[] unidentifiedAccessKey, final String pin, final String clientRegistrationLock, final String accountRecoveryPassword, final boolean unrestrictedUnidentifiedAccess, final long lastSeen) {

    assertAll(name,
        () -> assertEquals(discoverableByPhoneNumber, account.isDiscoverableByPhoneNumber()),
        () -> assertArrayEquals(currentProfileVersion, account.getCurrentProfileVersion().orElseThrow()),
        () -> assertEquals(identityKey, account.getAccountIdentityKey()),
        () -> assertArrayEquals(unidentifiedAccessKey, account.getUnidentifiedAccessKey().orElseThrow()),
        () -> assertTrue(account.getPhoneNumberIdentifier().isEmpty() || account.getRegistrationLock().verify(clientRegistrationLock)),
        () -> assertTrue(account.getAccountRecoveryPassword().orElseThrow().verify(accountRecoveryPassword)),
        () -> assertEquals(unrestrictedUnidentifiedAccess, account.isUnrestrictedUnidentifiedAccess())
    );
  }

  private CompletableFuture<?> modifyAccount(final UUID uuid, final Consumer<Account> accountMutation) {
    return CompletableFuture.runAsync(() -> accountsManager.update(uuid, accountMutation), mutationExecutor);
  }

  private CompletableFuture<?> modifyDevice(final UUID uuid, final byte deviceId, final Consumer<Device> deviceMutation) {
    return CompletableFuture.runAsync(() -> accountsManager.updateDevice(uuid, deviceId, deviceMutation), mutationExecutor);
  }

}
