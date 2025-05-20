/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

class AccountsManagerUsernameIntegrationTest {

  private static final String BASE_64_URL_USERNAME_HASH_1 = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final String BASE_64_URL_USERNAME_HASH_2 = "NLUom-CHwtemcdvOTTXdmXmzRIV7F05leS8lwkVK_vc";
  private static final String BASE_64_URL_ENCRYPTED_USERNAME_1 = "md1votbj9r794DsqTNrBqA";
  private static final String BASE_64_URL_ENCRYPTED_USERNAME_2 = "9hrqVLy59bzgPse-S9NUsA";
  private static final byte[] USERNAME_HASH_1 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_1);
  private static final byte[] USERNAME_HASH_2 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_2);
  private static final byte[] ENCRYPTED_USERNAME_1 = Base64.getUrlDecoder().decode(BASE_64_URL_ENCRYPTED_USERNAME_1);
  private static final byte[] ENCRYPTED_USERNAME_2 = Base64.getUrlDecoder().decode(BASE_64_URL_ENCRYPTED_USERNAME_2);

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.ACCOUNTS,
      Tables.NUMBERS,
      Tables.USERNAMES,
      Tables.DELETED_ACCOUNTS,
      Tables.PNI,
      Tables.PNI_ASSIGNMENTS,
      Tables.EC_KEYS,
      Tables.PQ_KEYS,
      Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS);

  @RegisterExtension
  static RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension("testbucket");

  private AccountsManager accountsManager;
  private Accounts accounts;

  @BeforeEach
  void setup() throws Exception {
    buildAccountsManager(1, 2, 10);
  }

  private void buildAccountsManager(final int initialWidth, int discriminatorMaxWidth, int attemptsPerWidth)
      throws Exception {
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    final DynamoDbAsyncClient dynamoDbAsyncClient = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient();
    final KeysManager keysManager = new KeysManager(
        new SingleUseECPreKeyStore(dynamoDbAsyncClient, DynamoDbExtensionSchema.Tables.EC_KEYS.tableName()),
        new SingleUseKEMPreKeyStore(dynamoDbAsyncClient, DynamoDbExtensionSchema.Tables.PQ_KEYS.tableName()),
        new PagedSingleUseKEMPreKeyStore(dynamoDbAsyncClient,
            S3_EXTENSION.getS3Client(),
            DynamoDbExtensionSchema.Tables.PAGED_PQ_KEYS.tableName(),
            S3_EXTENSION.getBucketName()),
        new RepeatedUseECSignedPreKeyStore(dynamoDbAsyncClient,
            DynamoDbExtensionSchema.Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS.tableName()),
        new RepeatedUseKEMSignedPreKeyStore(dynamoDbAsyncClient,
            DynamoDbExtensionSchema.Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS.tableName()));

    accounts = Mockito.spy(new Accounts(
        Clock.systemUTC(),
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName()));

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

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.PNI.tableName());

    final MessagesManager messageManager = mock(MessagesManager.class);
    final ProfilesManager profileManager = mock(ProfilesManager.class);
    when(messageManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(profileManager.deleteAll(any())).thenReturn(CompletableFuture.completedFuture(null));

    final DisconnectionRequestManager disconnectionRequestManager = mock(DisconnectionRequestManager.class);
    when(disconnectionRequestManager.requestDisconnection(any())).thenReturn(CompletableFuture.completedFuture(null));

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        CACHE_CLUSTER_EXTENSION.getRedisCluster(),
        mock(FaultTolerantRedisClient.class),
        accountLockManager,
        keysManager,
        messageManager,
        profileManager,
        mock(SecureStorageClient.class),
        mock(SecureValueRecovery2Client.class),
        disconnectionRequestManager,
        mock(RegistrationRecoveryPasswordsManager.class),
        mock(ClientPublicKeysManager.class),
        Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadScheduledExecutor(),
        mock(Clock.class),
        "link-device-secret".getBytes(StandardCharsets.UTF_8),
        dynamicConfigurationManager);
  }

  @Test
  void testNoUsernames() throws InterruptedException {
    final Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    List<byte[]> usernameHashes = List.of(USERNAME_HASH_1, USERNAME_HASH_2);
    int i = 0;
    for (byte[] hash : usernameHashes) {
      final Map<String, AttributeValue> item = new HashMap<>(Map.of(
          Accounts.UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(UUID.randomUUID()),
          Accounts.UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(hash)));
      // half of these are taken usernames, half are only reservations (have a TTL)
      if (i % 2 == 0) {
        item.put(Accounts.UsernameTable.ATTR_TTL,
            AttributeValues.fromLong(Instant.now().plus(Duration.ofMinutes(1)).getEpochSecond()));
      }
      i++;
      DYNAMO_DB_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
          .tableName(Tables.USERNAMES.tableName())
          .item(item)
          .build());
    }

    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accountsManager.reserveUsernameHash(account, usernameHashes));

    assertThat(accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getUsernameHash()).isEmpty();
  }

  @Test
  void testReserveUsernameGetFirstAvailableChoice() throws InterruptedException, UsernameHashNotAvailableException {
    final Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    ArrayList<byte[]> usernameHashes = new ArrayList<>(Arrays.asList(USERNAME_HASH_1, USERNAME_HASH_2));
    for (byte[] hash : usernameHashes) {
      DYNAMO_DB_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
          .tableName(Tables.USERNAMES.tableName())
          .item(Map.of(
              Accounts.UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(UUID.randomUUID()),
              Accounts.UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(hash)))
          .build());
    }


    byte[] availableHash = TestRandomUtil.nextBytes(32);
    usernameHashes.add(availableHash);
    usernameHashes.add(TestRandomUtil.nextBytes(32));

    final byte[] username = accountsManager
        .reserveUsernameHash(account, usernameHashes)
        .join()
        .reservedUsernameHash();

    assertArrayEquals(username, availableHash);
  }

  @Test
  public void testReserveConfirmClear() throws InterruptedException {
    Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    // reserve
    AccountsManager.UsernameReservation reservation =
        accountsManager.reserveUsernameHash(account, List.of(USERNAME_HASH_1)).join();

    assertArrayEquals(reservation.account().getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(accountsManager.getByUsernameHash(reservation.reservedUsernameHash()).join()).isEmpty();

    // confirm
    account = accountsManager.confirmReservedUsernameHash(
        reservation.account(),
        reservation.reservedUsernameHash(),
        ENCRYPTED_USERNAME_1).join();
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join().orElseThrow().getUuid()).isEqualTo(
        account.getUuid());
    assertThat(account.getUsernameLinkHandle()).isNotNull();
    assertThat(accountsManager.getByUsernameLinkHandle(account.getUsernameLinkHandle()).join().orElseThrow().getUuid())
        .isEqualTo(account.getUuid());

    // clear
    account = accountsManager.clearUsernameHash(account).join();
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getUsernameHash()).isEmpty();
  }

  @Test
  public void testHold() throws InterruptedException {
    Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    AccountsManager.UsernameReservation reservation =
        accountsManager.reserveUsernameHash(account, List.of(USERNAME_HASH_1)).join();

    // confirm
    account = accountsManager.confirmReservedUsernameHash(
        reservation.account(),
        reservation.reservedUsernameHash(),
        ENCRYPTED_USERNAME_1).join();

    // clear
    account = accountsManager.clearUsernameHash(account).join();
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getUsernameHash()).isEmpty();

    assertThat(accountsManager.getByUsernameHash(reservation.reservedUsernameHash()).join()).isEmpty();

    Account account2 = AccountsHelper.createAccount(accountsManager, "+18005552222");
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accountsManager.reserveUsernameHash(account2, List.of(USERNAME_HASH_1)),
        "account2 should not be able to reserve a held hash");
  }

  @Test
  public void testReservationLapsed() throws InterruptedException {
    final Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    AccountsManager.UsernameReservation reservation1 =
        accountsManager.reserveUsernameHash(account, List.of(USERNAME_HASH_1)).join();

    long past = Instant.now().minus(Duration.ofMinutes(1)).getEpochSecond();
    // force expiration
    DYNAMO_DB_EXTENSION.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
        .tableName(Tables.USERNAMES.tableName())
        .key(Map.of(Accounts.UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(USERNAME_HASH_1)))
        .updateExpression("SET #ttl = :ttl")
        .expressionAttributeNames(Map.of("#ttl", Accounts.UsernameTable.ATTR_TTL))
        .expressionAttributeValues(Map.of(":ttl", AttributeValues.fromLong(past)))
        .build());

    // a different account should be able to reserve it
    Account account2 = AccountsHelper.createAccount(accountsManager, "+18005552222");

    final AccountsManager.UsernameReservation reservation2 =
        accountsManager.reserveUsernameHash(account2, List.of(USERNAME_HASH_1)).join();
    assertArrayEquals(reservation2.reservedUsernameHash(), USERNAME_HASH_1);

    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accountsManager.confirmReservedUsernameHash(reservation1.account(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    account2 = accountsManager.confirmReservedUsernameHash(reservation2.account(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    assertEquals(accountsManager.getByUsernameHash(USERNAME_HASH_1).join().orElseThrow().getUuid(), account2.getUuid());
    assertArrayEquals(account2.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
  }

  @Test
  void testUsernameSetReserveAnotherClearSetReserved() throws InterruptedException {
    Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    // Set username hash
    final AccountsManager.UsernameReservation reservation1 =
        accountsManager.reserveUsernameHash(account, List.of(USERNAME_HASH_1)).join();

    account = accountsManager.confirmReservedUsernameHash(reservation1.account(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();

    // Reserve another hash on the same account
    final AccountsManager.UsernameReservation reservation2 =
        accountsManager.reserveUsernameHash(account, List.of(USERNAME_HASH_2)).join();

    account = reservation2.account();

    assertArrayEquals(account.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_2);
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertArrayEquals(account.getEncryptedUsername().orElseThrow(), ENCRYPTED_USERNAME_1);

    // Clear the set username hash but not the reserved one
    account = accountsManager.clearUsernameHash(account).join();
    assertThat(account.getReservedUsernameHash()).isPresent();
    assertThat(account.getUsernameHash()).isEmpty();

    // Confirm second reservation
    account = accountsManager.confirmReservedUsernameHash(account, reservation2.reservedUsernameHash(), ENCRYPTED_USERNAME_2).join();
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_2);
    assertArrayEquals(account.getEncryptedUsername().orElseThrow(), ENCRYPTED_USERNAME_2);
  }

  @Test
  public void testReclaim() throws InterruptedException {
    Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");
    final AccountsManager.UsernameReservation reservation1 =
        accountsManager.reserveUsernameHash(account, List.of(USERNAME_HASH_1)).join();
    account = accountsManager.confirmReservedUsernameHash(reservation1.account(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1)
        .join();

    // "reclaim" the account by re-registering
    Account reclaimed = AccountsHelper.createAccount(accountsManager, "+18005551111");

    // the username should still be reserved, but no longer on our account.
    assertThat(reclaimed.getUsernameHash()).isEmpty();

    // Make sure we can't lookup the account
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    // confirm it again
    accountsManager.confirmReservedUsernameHash(reclaimed, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isPresent();
  }

  @Test
  public void testUsernameLinks() throws InterruptedException, AccountAlreadyExistsException {
    final Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    account.setUsernameHash(TestRandomUtil.nextBytes(16));
    accounts.create(account, Collections.emptyList());

    final UUID linkHandle = UUID.randomUUID();
    final byte[] encryptedUsername = TestRandomUtil.nextBytes(32);
    accountsManager.update(account, a -> a.setUsernameLinkDetails(linkHandle, encryptedUsername));

    final Optional<Account> maybeAccount = accountsManager.getByUsernameLinkHandle(linkHandle).join();
    assertTrue(maybeAccount.isPresent());
    assertTrue(maybeAccount.get().getEncryptedUsername().isPresent());
    assertArrayEquals(encryptedUsername, maybeAccount.get().getEncryptedUsername().get());

    // making some unrelated change and updating account to check that username link data is still there
    final Optional<Account> accountToChange = accountsManager.getByAccountIdentifier(account.getUuid());
    assertTrue(accountToChange.isPresent());
    accountsManager.update(accountToChange.get(), a -> a.setDiscoverableByPhoneNumber(!a.isDiscoverableByPhoneNumber()));
    final Optional<Account> accountAfterChange = accountsManager.getByUsernameLinkHandle(linkHandle).join();
    assertTrue(accountAfterChange.isPresent());
    assertTrue(accountAfterChange.get().getEncryptedUsername().isPresent());
    assertArrayEquals(encryptedUsername, accountAfterChange.get().getEncryptedUsername().get());

    // now deleting the link
    final Optional<Account> accountToDeleteLink = accountsManager.getByAccountIdentifier(account.getUuid());
    accountsManager.update(accountToDeleteLink.orElseThrow(), a -> a.setUsernameLinkDetails(null, null));
    assertTrue(accounts.getByUsernameLinkHandle(linkHandle).join().isEmpty());
  }
}
