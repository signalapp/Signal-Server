/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.ThrowingSupplier;
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
      Tables.PAGED_PQ_KEYS,
      Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS,
      Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS,
      Tables.REDEEMED_RECEIPTS,
      Tables.PHONE_NUMBER_RECOVERY_PASSWORDS);

  @RegisterExtension
  static RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension("testbucket");

  private AccountsManager accountsManager;
  private Accounts accounts;

  @BeforeEach
  void setup() throws Exception {
    final DynamoDbAsyncClient dynamoDbAsyncClient = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient();
    final KeysManager keysManager = new KeysManager(
        new SingleUseECPreKeyStore(dynamoDbAsyncClient, DynamoDbExtensionSchema.Tables.EC_KEYS.tableName()),
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
        new RedeemedReceiptsManager(Clock.systemUTC(), Tables.REDEEMED_RECEIPTS.tableName(),
            DYNAMO_DB_EXTENSION.getDynamoDbClient(), Duration.ofDays(30)),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName()));

    final AccountLockManager accountLockManager = mock(AccountLockManager.class);

    doAnswer(invocation -> {
      final ThrowingSupplier<?, ?> task = invocation.getArgument(1);
      return task.get();
    }).when(accountLockManager).withLock(anySet(), any(), any());

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.PNI.tableName());

    final MessagesManager messageManager = mock(MessagesManager.class);
    final ProfilesManager profileManager = mock(ProfilesManager.class);
    when(messageManager.clear(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(profileManager.deleteAll(any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));

    final DisconnectionRequestManager disconnectionRequestManager = mock(DisconnectionRequestManager.class);
    when(disconnectionRequestManager.requestDisconnection(any())).thenReturn(CompletableFuture.completedFuture(null));

    final PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager =
        new PhoneNumberRecoveryPasswordsManager(new PhoneNumberRecoveryPasswords(
            Tables.PHONE_NUMBER_RECOVERY_PASSWORDS.tableName(),
            Duration.ofDays(1),
            DYNAMO_DB_EXTENSION.getDynamoDbClient(),
            Clock.systemUTC()));

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        CACHE_CLUSTER_EXTENSION.getRedisCluster(),
        mock(FaultTolerantRedisClient.class),
        accountLockManager,
        keysManager,
        messageManager,
        profileManager,
        mock(ChangeNumberWaitingPeriodManager.class),
        mock(SecureStorageClient.class),
        mock(SecureValueRecoveryClient.class),
        disconnectionRequestManager,
        phoneNumberRecoveryPasswordsManager,
        Executors.newSingleThreadExecutor(),
        Executors.newSingleThreadScheduledExecutor(),
        Executors.newSingleThreadScheduledExecutor(),
        Clock.systemUTC(),
        "link-device-secret".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void testNoUsernames() {
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

    assertThrows(UsernameHashNotAvailableException.class,
        () -> accountsManager.reserveUsernameHash(account.getAccountIdentifier(), usernameHashes));

    assertThat(accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getUsernameHash()).isEmpty();
  }

  @Test
  void testReserveUsernameGetFirstAvailableChoice() throws UsernameHashNotAvailableException {
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
        .reserveUsernameHash(account.getAccountIdentifier(), usernameHashes)
        .reservedUsernameHash();

    assertArrayEquals(username, availableHash);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReserveConfirmClear(final boolean numberless)
      throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    Account account = new AccountsHelper.AccountBuilder(accountsManager)
        .e164(numberless ? null : "+18005551111")
        .build();

    // reserve
    AccountsManager.UsernameReservation reservation =
        accountsManager.reserveUsernameHash(account.getAccountIdentifier(), List.of(USERNAME_HASH_1));

    assertArrayEquals(USERNAME_HASH_1, reservation.account().getReservedUsernameHash().orElseThrow());
    assertThat(accountsManager.getByUsernameHash(reservation.reservedUsernameHash()).join()).isEmpty();

    // confirm
    account = accountsManager.confirmReservedUsernameHash(
        reservation.account().getAccountIdentifier(),
        reservation.reservedUsernameHash(),
        ENCRYPTED_USERNAME_1);
    assertArrayEquals(USERNAME_HASH_1, account.getUsernameHash().orElseThrow());
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join().orElseThrow().getAccountIdentifier()).isEqualTo(
        account.getAccountIdentifier());
    assertThat(account.getUsernameLinkHandle()).isNotNull();
    assertThat(accountsManager.getByUsernameLinkHandle(account.getUsernameLinkHandle()).join().orElseThrow().getAccountIdentifier())
        .isEqualTo(account.getAccountIdentifier());

    // clear
    account = accountsManager.clearUsernameHash(account.getAccountIdentifier());
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getUsernameHash()).isEmpty();
  }

  @Test
  public void testHold()
      throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    AccountsManager.UsernameReservation reservation =
        accountsManager.reserveUsernameHash(account.getAccountIdentifier(), List.of(USERNAME_HASH_1));

    // confirm
    account = accountsManager.confirmReservedUsernameHash(
        reservation.account().getAccountIdentifier(),
        reservation.reservedUsernameHash(),
        ENCRYPTED_USERNAME_1);

    // clear
    account = accountsManager.clearUsernameHash(account.getAccountIdentifier());
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(accountsManager.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getUsernameHash()).isEmpty();

    assertThat(accountsManager.getByUsernameHash(reservation.reservedUsernameHash()).join()).isEmpty();

    Account account2 = AccountsHelper.createAccount(accountsManager, "+18005552222");
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accountsManager.reserveUsernameHash(account2.getAccountIdentifier(), List.of(USERNAME_HASH_1)),
        "account2 should not be able to reserve a held hash");
  }

  @Test
  public void testReservationLapsed()
      throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    AccountsManager.UsernameReservation reservation1 =
        accountsManager.reserveUsernameHash(account.getAccountIdentifier(), List.of(USERNAME_HASH_1));

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
        accountsManager.reserveUsernameHash(account2.getAccountIdentifier(), List.of(USERNAME_HASH_1));
    assertArrayEquals(USERNAME_HASH_1, reservation2.reservedUsernameHash());

    assertThrows(UsernameHashNotAvailableException.class,
        () -> accountsManager.confirmReservedUsernameHash(reservation1.account().getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    account2 = accountsManager.confirmReservedUsernameHash(reservation2.account().getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    assertEquals(accountsManager.getByUsernameHash(USERNAME_HASH_1).join().orElseThrow().getAccountIdentifier(), account2.getAccountIdentifier());
    assertArrayEquals(USERNAME_HASH_1, account2.getUsernameHash().orElseThrow());
  }

  @Test
  void testUsernameSetReserveAnotherClearSetReserved()
      throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");

    // Set username hash
    final AccountsManager.UsernameReservation reservation1 =
        accountsManager.reserveUsernameHash(account.getAccountIdentifier(), List.of(USERNAME_HASH_1));

    account = accountsManager.confirmReservedUsernameHash(reservation1.account().getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    // Reserve another hash on the same account
    final AccountsManager.UsernameReservation reservation2 =
        accountsManager.reserveUsernameHash(account.getAccountIdentifier(), List.of(USERNAME_HASH_2));

    account = reservation2.account();

    assertArrayEquals(USERNAME_HASH_2, account.getReservedUsernameHash().orElseThrow());
    assertArrayEquals(USERNAME_HASH_1, account.getUsernameHash().orElseThrow());
    assertArrayEquals(ENCRYPTED_USERNAME_1, account.getEncryptedUsername().orElseThrow());

    // Clear the set username hash but not the reserved one
    account = accountsManager.clearUsernameHash(account.getAccountIdentifier());
    assertThat(account.getReservedUsernameHash()).isPresent();
    assertThat(account.getUsernameHash()).isEmpty();

    // Confirm second reservation
    account = accountsManager.confirmReservedUsernameHash(account.getAccountIdentifier(), reservation2.reservedUsernameHash(), ENCRYPTED_USERNAME_2);
    assertArrayEquals(USERNAME_HASH_2, account.getUsernameHash().orElseThrow());
    assertArrayEquals(ENCRYPTED_USERNAME_2, account.getEncryptedUsername().orElseThrow());
  }

  @Test
  public void testReclaim()
      throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final Account account = AccountsHelper.createAccount(accountsManager, "+18005551111");
    final AccountsManager.UsernameReservation reservation1 =
        accountsManager.reserveUsernameHash(account.getAccountIdentifier(), List.of(USERNAME_HASH_1));
    accountsManager.confirmReservedUsernameHash(reservation1.account().getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    // "reclaim" the account by re-registering
    Account reclaimed = AccountsHelper.createAccount(accountsManager, "+18005551111");

    // the username should still be reserved, but no longer on our account.
    assertThat(reclaimed.getUsernameHash()).isEmpty();

    // Make sure we can't lookup the account
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    // confirm it again
    accountsManager.confirmReservedUsernameHash(reclaimed.getAccountIdentifier(), USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).join()).isPresent();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testUsernameLinks(final boolean numberless)
      throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final UUID accountIdentifier;
    {
      final Account account = new AccountsHelper.AccountBuilder(accountsManager)
          .e164(numberless ? null : "+18005551111")
          .build();
      accountIdentifier = account.getAccountIdentifier();
    }

    final AccountsManager.UsernameReservation reservation =
        accountsManager.reserveUsernameHash(accountIdentifier, List.of(USERNAME_HASH_1));

    accountsManager.confirmReservedUsernameHash(accountIdentifier, reservation.reservedUsernameHash(), ENCRYPTED_USERNAME_1);

    final UUID linkHandle = UUID.randomUUID();
    final byte[] encryptedUsername = TestRandomUtil.nextBytes(32);
    accountsManager.update(accountIdentifier, account -> account.setUsernameLinkDetails(linkHandle, encryptedUsername));

    final Optional<Account> maybeAccount = accountsManager.getByUsernameLinkHandle(linkHandle).join();
    assertTrue(maybeAccount.isPresent());
    assertTrue(maybeAccount.get().getEncryptedUsername().isPresent());
    assertArrayEquals(encryptedUsername, maybeAccount.get().getEncryptedUsername().get());

    // making some unrelated change and updating account to check that username link data is still there
    final Optional<Account> accountToChange = accountsManager.getByAccountIdentifier(accountIdentifier);
    assertTrue(accountToChange.isPresent());
    accountsManager.update(accountToChange.get().getAccountIdentifier(), a -> a.setDiscoverableByPhoneNumber(!a.isDiscoverableByPhoneNumber()));
    final Optional<Account> accountAfterChange = accountsManager.getByUsernameLinkHandle(linkHandle).join();
    assertTrue(accountAfterChange.isPresent());
    assertTrue(accountAfterChange.get().getEncryptedUsername().isPresent());
    assertArrayEquals(encryptedUsername, accountAfterChange.get().getEncryptedUsername().get());

    // now deleting the link
    final Optional<Account> accountToDeleteLink = accountsManager.getByAccountIdentifier(accountIdentifier);
    accountsManager.update(accountToDeleteLink.orElseThrow().getAccountIdentifier(), a -> a.setUsernameLinkDetails(null, null));
    assertTrue(accounts.getByUsernameLinkHandle(linkHandle).join().isEmpty());
  }
}
