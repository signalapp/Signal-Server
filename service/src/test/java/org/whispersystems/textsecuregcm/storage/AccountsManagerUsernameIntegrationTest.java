/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
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
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

class AccountsManagerUsernameIntegrationTest {

  private static final String ACCOUNTS_TABLE_NAME = "accounts_test";
  private static final String NUMBERS_TABLE_NAME = "numbers_test";
  private static final String PNI_ASSIGNMENT_TABLE_NAME = "pni_assignment_test";
  private static final String USERNAMES_TABLE_NAME = "usernames_test";
  private static final String PNI_TABLE_NAME = "pni_test";
  private static final String BASE_64_URL_USERNAME_HASH_1 = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final String BASE_64_URL_USERNAME_HASH_2 = "NLUom-CHwtemcdvOTTXdmXmzRIV7F05leS8lwkVK_vc";
  private static final int SCAN_PAGE_SIZE = 1;
  private static final byte[] USERNAME_HASH_1 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_1);
  private static final byte[] USERNAME_HASH_2 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_2);

  @RegisterExtension
  static DynamoDbExtension ACCOUNTS_DYNAMO_EXTENSION = DynamoDbExtension.builder()
      .tableName(ACCOUNTS_TABLE_NAME)
      .hashKey(Accounts.KEY_ACCOUNT_UUID)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(Accounts.KEY_ACCOUNT_UUID)
          .attributeType(ScalarAttributeType.B)
          .build())
      .build();

  @RegisterExtension
  static DynamoDbExtension PNI_DYNAMO_EXTENSION = DynamoDbExtension.builder()
      .tableName(PNI_TABLE_NAME)
      .hashKey(PhoneNumberIdentifiers.KEY_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(PhoneNumberIdentifiers.KEY_E164)
          .attributeType(ScalarAttributeType.S)
          .build())
      .build();

  @RegisterExtension
  static RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private AccountsManager accountsManager;
  private Accounts accounts;

  @BeforeEach
  void setup() throws InterruptedException {
    CreateTableRequest createNumbersTableRequest = CreateTableRequest.builder()
        .tableName(NUMBERS_TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName(Accounts.ATTR_ACCOUNT_E164)
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(Accounts.ATTR_ACCOUNT_E164)
            .attributeType(ScalarAttributeType.S)
            .build())
        .provisionedThroughput(DynamoDbExtension.DEFAULT_PROVISIONED_THROUGHPUT)
        .build();

    ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().createTable(createNumbersTableRequest);
    CreateTableRequest createUsernamesTableRequest = CreateTableRequest.builder()
        .tableName(USERNAMES_TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName(Accounts.ATTR_USERNAME_HASH)
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(Accounts.ATTR_USERNAME_HASH)
            .attributeType(ScalarAttributeType.B)
            .build())
        .provisionedThroughput(DynamoDbExtension.DEFAULT_PROVISIONED_THROUGHPUT)
        .build();

    ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().createTable(createUsernamesTableRequest);
    CreateTableRequest createPhoneNumberIdentifierTableRequest = CreateTableRequest.builder()
        .tableName(PNI_ASSIGNMENT_TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName(Accounts.ATTR_PNI_UUID)
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(Accounts.ATTR_PNI_UUID)
            .attributeType(ScalarAttributeType.B)
            .build())
        .provisionedThroughput(DynamoDbExtension.DEFAULT_PROVISIONED_THROUGHPUT)
        .build();

    ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().createTable(createPhoneNumberIdentifierTableRequest);
    buildAccountsManager(1, 2, 10);
  }

  private void buildAccountsManager(final int initialWidth, int discriminatorMaxWidth, int attemptsPerWidth)
      throws InterruptedException {
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    accounts = Mockito.spy(new Accounts(
        ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient(),
        ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbAsyncClient(),
        ACCOUNTS_DYNAMO_EXTENSION.getTableName(),
        NUMBERS_TABLE_NAME,
        PNI_ASSIGNMENT_TABLE_NAME,
        USERNAMES_TABLE_NAME,
        SCAN_PAGE_SIZE));

    final DeletedAccountsManager deletedAccountsManager = mock(DeletedAccountsManager.class);
    doAnswer((final InvocationOnMock invocationOnMock) -> {
      @SuppressWarnings("unchecked")
      Consumer<Optional<UUID>> consumer = invocationOnMock.getArgument(1, Consumer.class);
      consumer.accept(Optional.empty());
      return null;
    }).when(deletedAccountsManager).lockAndTake(any(), any());

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(PNI_DYNAMO_EXTENSION.getDynamoDbClient(), PNI_TABLE_NAME);

    final ExperimentEnrollmentManager experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);
    when(experimentEnrollmentManager.isEnrolled(any(UUID.class), eq(AccountsManager.USERNAME_EXPERIMENT_NAME)))
        .thenReturn(true);
    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        CACHE_CLUSTER_EXTENSION.getRedisCluster(),
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
        experimentEnrollmentManager,
        mock(RegistrationRecoveryPasswordsManager.class),
        mock(Clock.class));
  }

  @Test
  void testNoUsernames() throws InterruptedException {
    Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());
    List<byte[]> usernameHashes = List.of(USERNAME_HASH_1, USERNAME_HASH_2);
    int i = 0;
    for (byte[] hash : usernameHashes) {
      final Map<String, AttributeValue> item = new HashMap<>(Map.of(
          Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(UUID.randomUUID()),
          Accounts.ATTR_USERNAME_HASH, AttributeValues.fromByteArray(hash)));
      // half of these are taken usernames, half are only reservations (have a TTL)
      if (i % 2 == 0) {
        item.put(Accounts.ATTR_TTL,
            AttributeValues.fromLong(Instant.now().plus(Duration.ofMinutes(1)).getEpochSecond()));
      }
      i++;
      ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
          .tableName(USERNAMES_TABLE_NAME)
          .item(item)
          .build());
    }
    assertThrows(UsernameHashNotAvailableException.class, () -> {accountsManager.reserveUsernameHash(account, usernameHashes);});
    assertThat(accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getUsernameHash()).isEmpty();
  }

  @Test
  void testReserveUsernameSnatched() throws InterruptedException, UsernameHashNotAvailableException {
    final Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());
    ArrayList<byte[]> usernameHashes = new ArrayList<>(Arrays.asList(USERNAME_HASH_1, USERNAME_HASH_2));
    for (byte[] hash : usernameHashes) {
      ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
          .tableName(USERNAMES_TABLE_NAME)
          .item(Map.of(
              Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(UUID.randomUUID()),
              Accounts.ATTR_USERNAME_HASH, AttributeValues.fromByteArray(hash)))
          .build());
    }


    byte[] availableHash = new byte[32];
    new SecureRandom().nextBytes(availableHash);
    usernameHashes.add(availableHash);

    // first time this is called lie and say the username is available
    // this simulates seeing an available username and then it being taken
    // by someone before the write
    doReturn(true).doCallRealMethod().when(accounts).usernameHashAvailable(any());
    final byte[] username = accountsManager
        .reserveUsernameHash(account, usernameHashes)
        .reservedUsernameHash();

    assertArrayEquals(username, availableHash);

    // 1 attempt on first try (returns true),
    // 5 more attempts until "availableHash" returns true
    verify(accounts, times(4)).usernameHashAvailable(any());
  }

  @Test
  public void testReserveConfirmClear()
      throws InterruptedException, UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());

    // reserve
    AccountsManager.UsernameReservation reservation = accountsManager.reserveUsernameHash(account, List.of(
        USERNAME_HASH_1));
    assertArrayEquals(reservation.account().getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(accountsManager.getByUsernameHash(reservation.reservedUsernameHash())).isEmpty();

    // confirm
    account = accountsManager.confirmReservedUsernameHash(
        reservation.account(),
        reservation.reservedUsernameHash());
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1).orElseThrow().getUuid()).isEqualTo(
        account.getUuid());

    // clear
    account = accountsManager.clearUsernameHash(account);
    assertThat(accountsManager.getByUsernameHash(USERNAME_HASH_1)).isEmpty();
    assertThat(accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getUsernameHash()).isEmpty();
  }

  @Test
  public void testReservationLapsed()
      throws InterruptedException, UsernameHashNotAvailableException, UsernameReservationNotFoundException {

    final Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());
    AccountsManager.UsernameReservation reservation1 = accountsManager.reserveUsernameHash(account, List.of(
        USERNAME_HASH_1));

    long past = Instant.now().minus(Duration.ofMinutes(1)).getEpochSecond();
    // force expiration
    ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
        .tableName(USERNAMES_TABLE_NAME)
        .key(Map.of(Accounts.ATTR_USERNAME_HASH, AttributeValues.fromByteArray(USERNAME_HASH_1)))
        .updateExpression("SET #ttl = :ttl")
        .expressionAttributeNames(Map.of("#ttl", Accounts.ATTR_TTL))
        .expressionAttributeValues(Map.of(":ttl", AttributeValues.fromLong(past)))
        .build());

    // a different account should be able to reserve it
    Account account2 = accountsManager.create("+18005552222", "password", null, new AccountAttributes(),
        new ArrayList<>());
    final AccountsManager.UsernameReservation reservation2 = accountsManager.reserveUsernameHash(account2, List.of(
        USERNAME_HASH_1));
    assertArrayEquals(reservation2.reservedUsernameHash(), USERNAME_HASH_1);

    assertThrows(UsernameHashNotAvailableException.class,
        () -> accountsManager.confirmReservedUsernameHash(reservation1.account(), USERNAME_HASH_1));
    account2 = accountsManager.confirmReservedUsernameHash(reservation2.account(), USERNAME_HASH_1);
    assertEquals(accountsManager.getByUsernameHash(USERNAME_HASH_1).orElseThrow().getUuid(), account2.getUuid());
    assertArrayEquals(account2.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
  }

  @Test
  void testUsernameSetReserveAnotherClearSetReserved()
      throws InterruptedException, UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());

    // Set username hash
    final AccountsManager.UsernameReservation reservation1 = accountsManager.reserveUsernameHash(account, List.of(
        USERNAME_HASH_1));
    account = accountsManager.confirmReservedUsernameHash(reservation1.account(), USERNAME_HASH_1);

    // Reserve another hash on the same account
    final AccountsManager.UsernameReservation reservation2 = accountsManager.reserveUsernameHash(account, List.of(
        USERNAME_HASH_2));
    account = reservation2.account();

    assertArrayEquals(account.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_2);
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);

    // Clear the set username hash but not the reserved one
    account = accountsManager.clearUsernameHash(account);
    assertThat(account.getReservedUsernameHash()).isPresent();
    assertThat(account.getUsernameHash()).isEmpty();

    // Confirm second reservation
    account = accountsManager.confirmReservedUsernameHash(account, reservation2.reservedUsernameHash());
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_2);
  }
}
