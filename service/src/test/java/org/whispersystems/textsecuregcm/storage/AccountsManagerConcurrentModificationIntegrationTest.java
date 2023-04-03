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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.JsonHelpers;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import org.whispersystems.textsecuregcm.util.Pair;


class AccountsManagerConcurrentModificationIntegrationTest {

  private static final int SCAN_PAGE_SIZE = 1;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.ACCOUNTS,
      Tables.NUMBERS,
      Tables.PNI_ASSIGNMENTS
  );

  private Accounts accounts;

  private AccountsManager accountsManager;

  private RedisAdvancedClusterCommands<String, String> commands;

  private Executor mutationExecutor = new ThreadPoolExecutor(20, 20, 5, TimeUnit.SECONDS, new LinkedBlockingDeque<>(20));

  @BeforeEach
  void setup() throws InterruptedException {

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    accounts = new Accounts(
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        SCAN_PAGE_SIZE);

    {
      //noinspection unchecked
      commands = mock(RedisAdvancedClusterCommands.class);

      final DeletedAccountsManager deletedAccountsManager = mock(DeletedAccountsManager.class);

      doAnswer(invocation -> {
        //noinspection unchecked
        invocation.getArgument(1, Consumer.class).accept(Optional.empty());
        return null;
      }).when(deletedAccountsManager).lockAndTake(anyString(), any());

      final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
      when(phoneNumberIdentifiers.getPhoneNumberIdentifier(anyString()))
          .thenAnswer((Answer<UUID>) invocation -> UUID.randomUUID());

      accountsManager = new AccountsManager(
          accounts,
          phoneNumberIdentifiers,
          RedisClusterHelper.builder().stringCommands(commands).build(),
          deletedAccountsManager,
          mock(DirectoryQueue.class),
          mock(Keys.class),
          mock(MessagesManager.class),
          mock(ProfilesManager.class),
          mock(StoredVerificationCodeManager.class),
          mock(SecureStorageClient.class),
          mock(SecureBackupClient.class),
          mock(SecureValueRecovery2Client.class),
          mock(ClientPresenceManager.class),
          mock(ExperimentEnrollmentManager.class),
          mock(RegistrationRecoveryPasswordsManager.class),
          mock(Clock.class)
      );
    }
  }

  @Test
  void testConcurrentUpdate() throws IOException, InterruptedException {

    final UUID uuid;
    {
      final Account account = accountsManager.update(
          accountsManager.create("+14155551212", "password", null, new AccountAttributes(), new ArrayList<>()),
          a -> {
            a.setUnidentifiedAccessKey(new byte[16]);

            final Random random = new Random();
            final SignedPreKey signedPreKey = new SignedPreKey(random.nextInt(), "testPublicKey-" + random.nextInt(),
                "testSignature-" + random.nextInt());

            a.removeDevice(1);
            a.addDevice(DevicesHelper.createDevice(1));
          });

      uuid = account.getUuid();
    }

    final boolean discoverableByPhoneNumber = false;
    final String currentProfileVersion = "cpv";
    final String identityKey = "ikey";
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
        modifyDevice(uuid, Device.MASTER_ID, device -> device.setLastSeen(lastSeen)),
        modifyDevice(uuid, Device.MASTER_ID, device -> device.setName("deviceName"))
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

  private void verifyAccount(final String name, final Account account, final boolean discoverableByPhoneNumber, final String currentProfileVersion, final String identityKey, final byte[] unidentifiedAccessKey, final String pin, final String clientRegistrationLock, final boolean unrestrictedUnidentifiedAccess, final long lastSeen) {

    assertAll(name,
        () -> assertEquals(discoverableByPhoneNumber, account.isDiscoverableByPhoneNumber()),
        () -> assertEquals(currentProfileVersion, account.getCurrentProfileVersion().orElseThrow()),
        () -> assertEquals(identityKey, account.getIdentityKey()),
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

  private CompletableFuture<?> modifyDevice(final UUID uuid, final long deviceId, final Consumer<Device> deviceMutation) {

    return CompletableFuture.runAsync(() -> {
      final Account account = accountsManager.getByAccountIdentifier(uuid).orElseThrow();
      accountsManager.updateDevice(account, deviceId, deviceMutation);
    }, mutationExecutor);
  }
}
