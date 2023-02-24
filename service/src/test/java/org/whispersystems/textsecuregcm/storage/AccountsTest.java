/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.uuid.UUIDComparator;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

class AccountsTest {

  private static final String ACCOUNTS_TABLE_NAME = "accounts_test";
  private static final String NUMBER_CONSTRAINT_TABLE_NAME = "numbers_test";
  private static final String PNI_CONSTRAINT_TABLE_NAME = "pni_test";
  private static final String USERNAME_CONSTRAINT_TABLE_NAME = "username_test";
  private static final String BASE_64_URL_USERNAME_HASH_1 = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final String BASE_64_URL_USERNAME_HASH_2 = "NLUom-CHwtemcdvOTTXdmXmzRIV7F05leS8lwkVK_vc";
  private static final byte[] USERNAME_HASH_1 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_1);
  private static final byte[] USERNAME_HASH_2 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_2);

  private static final int SCAN_PAGE_SIZE = 1;



  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(ACCOUNTS_TABLE_NAME)
      .hashKey(Accounts.KEY_ACCOUNT_UUID)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(Accounts.KEY_ACCOUNT_UUID)
          .attributeType(ScalarAttributeType.B)
          .build())
      .build();

  private final TestClock clock = TestClock.pinned(Instant.EPOCH);
  private DynamicConfigurationManager<DynamicConfiguration> mockDynamicConfigManager;
  private Accounts accounts;

  @BeforeEach
  void setupAccountsDao() {
    CreateTableRequest createNumbersTableRequest = CreateTableRequest.builder()
        .tableName(NUMBER_CONSTRAINT_TABLE_NAME)
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

    dynamoDbExtension.getDynamoDbClient().createTable(createNumbersTableRequest);

    CreateTableRequest createPhoneNumberIdentifierTableRequest = CreateTableRequest.builder()
        .tableName(PNI_CONSTRAINT_TABLE_NAME)
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

    dynamoDbExtension.getDynamoDbClient().createTable(createPhoneNumberIdentifierTableRequest);

    CreateTableRequest createUsernamesTableRequest = CreateTableRequest.builder()
        .tableName(USERNAME_CONSTRAINT_TABLE_NAME)
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

    dynamoDbExtension.getDynamoDbClient().createTable(createUsernamesTableRequest);

    @SuppressWarnings("unchecked") DynamicConfigurationManager<DynamicConfiguration> m = mock(DynamicConfigurationManager.class);
    mockDynamicConfigManager = m;

    when(mockDynamicConfigManager.getConfiguration())
        .thenReturn(new DynamicConfiguration());

    this.accounts = new Accounts(
        clock,
        dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getDynamoDbAsyncClient(),
        dynamoDbExtension.getTableName(),
        NUMBER_CONSTRAINT_TABLE_NAME,
        PNI_CONSTRAINT_TABLE_NAME,
        USERNAME_CONSTRAINT_TABLE_NAME,
        SCAN_PAGE_SIZE);
  }

  @Test
  void testStore() {
    Device device = generateDevice(1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    boolean freshUser = accounts.create(account);

    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    freshUser = accounts.create(account);
    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());
  }

  @Test
  void testStoreMulti() {
    final List<Device> devices = List.of(generateDevice(1), generateDevice(2));
    final Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), devices);

    accounts.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());
  }

  @Test
  void testRetrieve() {
    final List<Device> devicesFirst = List.of(generateDevice(1), generateDevice(2));

    UUID uuidFirst = UUID.randomUUID();
    UUID pniFirst = UUID.randomUUID();
    Account accountFirst = generateAccount("+14151112222", uuidFirst, pniFirst, devicesFirst);

    final List<Device> devicesSecond = List.of(generateDevice(1), generateDevice(2));

    UUID uuidSecond = UUID.randomUUID();
    UUID pniSecond = UUID.randomUUID();
    Account accountSecond = generateAccount("+14152221111", uuidSecond, pniSecond, devicesSecond);

    accounts.create(accountFirst);
    accounts.create(accountSecond);

    Optional<Account> retrievedFirst = accounts.getByE164("+14151112222");
    Optional<Account> retrievedSecond = accounts.getByE164("+14152221111");

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, null, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, null, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByAccountIdentifier(uuidFirst);
    retrievedSecond = accounts.getByAccountIdentifier(uuidSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, null, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, null, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByPhoneNumberIdentifier(pniFirst);
    retrievedSecond = accounts.getByPhoneNumberIdentifier(pniSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, null, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, null, retrievedSecond.get(), accountSecond);
  }

  @Test
  void testRetrieveNoPni() throws JsonProcessingException {
    final List<Device> devices = List.of(generateDevice(1), generateDevice(2));
    final UUID uuid = UUID.randomUUID();
    final Account account = generateAccount("+14151112222", uuid, null, devices);

    // Accounts#create enforces that newly-created accounts have a PNI, so we need to make a bit of an end-run around it
    // to simulate an existing account with no PNI.
    {
      final TransactWriteItem phoneNumberConstraintPut = TransactWriteItem.builder()
          .put(
              Put.builder()
                  .tableName(NUMBER_CONSTRAINT_TABLE_NAME)
                  .item(Map.of(
                      Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber()),
                      Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
                  .conditionExpression(
                      "attribute_not_exists(#number) OR (attribute_exists(#number) AND #uuid = :uuid)")
                  .expressionAttributeNames(
                      Map.of("#uuid", Accounts.KEY_ACCOUNT_UUID,
                          "#number", Accounts.ATTR_ACCOUNT_E164))
                  .expressionAttributeValues(
                      Map.of(":uuid", AttributeValues.fromUUID(account.getUuid())))
                  .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                  .build())
          .build();

      final TransactWriteItem accountPut = TransactWriteItem.builder()
          .put(Put.builder()
              .tableName(ACCOUNTS_TABLE_NAME)
              .conditionExpression("attribute_not_exists(#number) OR #number = :number")
              .expressionAttributeNames(Map.of("#number", Accounts.ATTR_ACCOUNT_E164))
              .expressionAttributeValues(Map.of(":number", AttributeValues.fromString(account.getNumber())))
              .item(Map.of(
                  Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                  Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber()),
                  Accounts.ATTR_ACCOUNT_DATA, AttributeValues.fromByteArray(SystemMapper.jsonMapper().writeValueAsBytes(account)),
                  Accounts.ATTR_VERSION, AttributeValues.fromInt(account.getVersion()),
                  Accounts.ATTR_CANONICALLY_DISCOVERABLE, AttributeValues.fromBool(account.shouldBeVisibleInDirectory())))
              .build())
          .build();

      dynamoDbExtension.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(phoneNumberConstraintPut, accountPut)
          .build());
    }

    Optional<Account> retrieved = accounts.getByE164("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", uuid, null, null, retrieved.get(), account);

    retrieved = accounts.getByAccountIdentifier(uuid);

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", uuid, null, null, retrieved.get(), account);
  }

  @Test
  void testOverwrite() {
    Device device = generateDevice(1);
    UUID firstUuid = UUID.randomUUID();
    UUID firstPni = UUID.randomUUID();
    Account account = generateAccount("+14151112222", firstUuid, firstPni, List.of(device));

    accounts.create(account);

    final SecureRandom byteGenerator = new SecureRandom();
    final byte[] usernameHash = new byte[32];
    byteGenerator.nextBytes(usernameHash);

    // Set up the existing account to have a username hash
    accounts.confirmUsernameHash(account, usernameHash);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), usernameHash, account, true);

    assertPhoneNumberConstraintExists("+14151112222", firstUuid);
    assertPhoneNumberIdentifierConstraintExists(firstPni, firstUuid);

    accounts.update(account);

    UUID secondUuid = UUID.randomUUID();

    device = generateDevice(1);
    account = generateAccount("+14151112222", secondUuid, UUID.randomUUID(), List.of(device));

    final boolean freshUser = accounts.create(account);
    assertThat(freshUser).isFalse();
    verifyStoredState("+14151112222", firstUuid, firstPni, usernameHash, account, true);

    assertPhoneNumberConstraintExists("+14151112222", firstUuid);
    assertPhoneNumberIdentifierConstraintExists(firstPni, firstUuid);

    device = generateDevice(1);
    Account invalidAccount = generateAccount("+14151113333", firstUuid, UUID.randomUUID(), List.of(device));

    assertThatThrownBy(() -> accounts.create(invalidAccount));
  }

  @Test
  void testUpdate() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    accounts.create(account);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    device.setName("foobar");

    accounts.update(account);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    Optional<Account> retrieved = accounts.getByE164("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);

    retrieved = accounts.getByAccountIdentifier(account.getUuid());

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    device = generateDevice(1);
    Account unknownAccount = generateAccount("+14151113333", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    assertThatThrownBy(() -> accounts.update(unknownAccount)).isInstanceOfAny(ConditionalCheckFailedException.class);

    accounts.update(account);

    assertThat(account.getVersion()).isEqualTo(2);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    account.setVersion(1);

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);

    account.setVersion(2);

    accounts.update(account);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdateWithMockTransactionConflictException(boolean wrapException) {

    final DynamoDbAsyncClient dynamoDbAsyncClient = mock(DynamoDbAsyncClient.class);
    accounts = new Accounts(mock(DynamoDbClient.class),
        dynamoDbAsyncClient, dynamoDbExtension.getTableName(),
        NUMBER_CONSTRAINT_TABLE_NAME, PNI_CONSTRAINT_TABLE_NAME, USERNAME_CONSTRAINT_TABLE_NAME, SCAN_PAGE_SIZE);

    Exception e = TransactionConflictException.builder().build();
    e = wrapException ? new CompletionException(e) : e;

    when(dynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(e));

    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID());

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);
  }

  @Test
  void testRetrieveFrom() {
    List<Account> users = new ArrayList<>();

    for (int i = 1; i <= 100; i++) {
      Account account = generateAccount("+1" + String.format("%03d", i), UUID.randomUUID(), UUID.randomUUID());
      users.add(account);
      accounts.create(account);
    }

    users.sort((account, t1) -> UUIDComparator.staticCompare(account.getUuid(), t1.getUuid()));

    AccountCrawlChunk retrieved = accounts.getAllFromStart(10);
    assertThat(retrieved.getAccounts().size()).isEqualTo(10);

    for (int i = 0; i < retrieved.getAccounts().size(); i++) {
      final Account retrievedAccount = retrieved.getAccounts().get(i);

      final Account expectedAccount = users.stream()
          .filter(account -> account.getUuid().equals(retrievedAccount.getUuid()))
          .findAny()
          .orElseThrow();

      verifyStoredState(expectedAccount.getNumber(), expectedAccount.getUuid(), expectedAccount.getPhoneNumberIdentifier(), null, retrievedAccount, expectedAccount);

      users.remove(expectedAccount);
    }

    for (int j = 0; j < 9; j++) {
      retrieved = accounts.getAllFrom(retrieved.getLastUuid().orElseThrow(), 10);
      assertThat(retrieved.getAccounts().size()).isEqualTo(10);

      for (int i = 0; i < retrieved.getAccounts().size(); i++) {
        final Account retrievedAccount = retrieved.getAccounts().get(i);

        final Account expectedAccount = users.stream()
            .filter(account -> account.getUuid().equals(retrievedAccount.getUuid()))
            .findAny()
            .orElseThrow();

        verifyStoredState(expectedAccount.getNumber(), expectedAccount.getUuid(), expectedAccount.getPhoneNumberIdentifier(), null, retrievedAccount, expectedAccount);

        users.remove(expectedAccount);
      }
    }

    assertThat(users).isEmpty();
  }

  @Test
  void testDelete() {
    final Device deletedDevice = generateDevice(1);
    final Account deletedAccount = generateAccount("+14151112222", UUID.randomUUID(),
        UUID.randomUUID(), List.of(deletedDevice));
    final Device retainedDevice = generateDevice(1);
    final Account retainedAccount = generateAccount("+14151112345", UUID.randomUUID(),
        UUID.randomUUID(), List.of(retainedDevice));

    accounts.create(deletedAccount);
    accounts.create(retainedAccount);

    assertPhoneNumberConstraintExists("+14151112222", deletedAccount.getUuid());
    assertPhoneNumberIdentifierConstraintExists(deletedAccount.getPhoneNumberIdentifier(), deletedAccount.getUuid());
    assertPhoneNumberConstraintExists("+14151112345", retainedAccount.getUuid());
    assertPhoneNumberIdentifierConstraintExists(retainedAccount.getPhoneNumberIdentifier(), retainedAccount.getUuid());

    assertThat(accounts.getByAccountIdentifier(deletedAccount.getUuid())).isPresent();
    assertThat(accounts.getByAccountIdentifier(retainedAccount.getUuid())).isPresent();

    accounts.delete(deletedAccount.getUuid());

    assertThat(accounts.getByAccountIdentifier(deletedAccount.getUuid())).isNotPresent();

    assertPhoneNumberConstraintDoesNotExist(deletedAccount.getNumber());
    assertPhoneNumberIdentifierConstraintDoesNotExist(deletedAccount.getPhoneNumberIdentifier());

    verifyStoredState(retainedAccount.getNumber(), retainedAccount.getUuid(), retainedAccount.getPhoneNumberIdentifier(),
        null, accounts.getByAccountIdentifier(retainedAccount.getUuid()).get(), retainedAccount);

    {
      final Account recreatedAccount = generateAccount(deletedAccount.getNumber(), UUID.randomUUID(),
          UUID.randomUUID(), List.of(generateDevice(1)));

      final boolean freshUser = accounts.create(recreatedAccount);

      assertThat(freshUser).isTrue();
      assertThat(accounts.getByAccountIdentifier(recreatedAccount.getUuid())).isPresent();
      verifyStoredState(recreatedAccount.getNumber(), recreatedAccount.getUuid(), recreatedAccount.getPhoneNumberIdentifier(),
          null, accounts.getByAccountIdentifier(recreatedAccount.getUuid()).get(), recreatedAccount);

      assertPhoneNumberConstraintExists(recreatedAccount.getNumber(), recreatedAccount.getUuid());
      assertPhoneNumberIdentifierConstraintExists(recreatedAccount.getPhoneNumberIdentifier(), recreatedAccount.getUuid());
    }
  }

  @Test
  void testMissing() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    accounts.create(account);

    Optional<Account> retrieved = accounts.getByE164("+11111111");
    assertThat(retrieved.isPresent()).isFalse();

    retrieved = accounts.getByAccountIdentifier(UUID.randomUUID());
    assertThat(retrieved.isPresent()).isFalse();
  }

  @Test
  void testCanonicallyDiscoverableSet() {
    Device device = generateDevice(1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));
    account.setDiscoverableByPhoneNumber(false);
    accounts.create(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, false);
    account.setDiscoverableByPhoneNumber(true);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);
    account.setDiscoverableByPhoneNumber(false);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, false);
  }

  @Test
  public void testChangeNumber() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Device device = generateDevice(1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), originalPni, List.of(device));

    accounts.create(account);

    assertThat(accounts.getByPhoneNumberIdentifier(originalPni)).isPresent();

    assertPhoneNumberConstraintExists(originalNumber, account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(originalPni, account.getUuid());

    {
      final Optional<Account> retrieved = accounts.getByE164(originalNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(originalNumber, account.getUuid(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);
    }

    accounts.changeNumber(account, targetNumber, targetPni);

    assertThat(accounts.getByE164(originalNumber)).isEmpty();
    assertThat(accounts.getByAccountIdentifier(originalPni)).isEmpty();

    assertPhoneNumberConstraintDoesNotExist(originalNumber);
    assertPhoneNumberIdentifierConstraintDoesNotExist(originalPni);
    assertPhoneNumberConstraintExists(targetNumber, account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(targetPni, account.getUuid());

    {
      final Optional<Account> retrieved = accounts.getByE164(targetNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(targetNumber, account.getUuid(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);

      assertThat(retrieved.get().getPhoneNumberIdentifier()).isEqualTo(targetPni);
      assertThat(accounts.getByPhoneNumberIdentifier(targetPni)).isPresent();
    }
  }

  @Test
  public void testChangeNumberConflict() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Device existingDevice = generateDevice(1);
    final Account existingAccount = generateAccount(targetNumber, UUID.randomUUID(), targetPni, List.of(existingDevice));

    final Device device = generateDevice(1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), originalPni, List.of(device));

    accounts.create(account);
    accounts.create(existingAccount);

    assertThrows(TransactionCanceledException.class, () -> accounts.changeNumber(account, targetNumber, targetPni));

    assertPhoneNumberConstraintExists(originalNumber, account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(originalPni, account.getUuid());
    assertPhoneNumberConstraintExists(targetNumber, existingAccount.getUuid());
    assertPhoneNumberIdentifierConstraintExists(targetPni, existingAccount.getUuid());
  }

  @Test
  public void testChangeNumberPhoneNumberIdentifierConflict() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final Device device = generateDevice(1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    accounts.create(account);

    final UUID existingAccountIdentifier = UUID.randomUUID();
    final UUID existingPhoneNumberIdentifier = UUID.randomUUID();

    // Artificially inject a conflicting PNI entry
    dynamoDbExtension.getDynamoDbClient().putItem(PutItemRequest.builder()
        .tableName(PNI_CONSTRAINT_TABLE_NAME)
        .item(Map.of(
            Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(existingPhoneNumberIdentifier),
            Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(existingAccountIdentifier)))
        .conditionExpression(
            "attribute_not_exists(#pni) OR (attribute_exists(#pni) AND #uuid = :uuid)")
        .expressionAttributeNames(
            Map.of("#uuid", Accounts.KEY_ACCOUNT_UUID,
                "#pni", Accounts.ATTR_PNI_UUID))
        .expressionAttributeValues(
            Map.of(":uuid", AttributeValues.fromUUID(existingAccountIdentifier)))
        .build());

    assertThrows(TransactionCanceledException.class, () -> accounts.changeNumber(account, targetNumber, existingPhoneNumberIdentifier));
  }

  @Test
  void testSwitchUsernameHashes() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1)).isEmpty();

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1);

    {
      final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_1);

      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(), USERNAME_HASH_1, maybeAccount.orElseThrow(), account);
    }

    accounts.reserveUsernameHash(account, USERNAME_HASH_2, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_2);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1)).isEmpty();
    assertThat(dynamoDbExtension.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(USERNAME_CONSTRAINT_TABLE_NAME)
            .key(Map.of(Accounts.ATTR_USERNAME_HASH, AttributeValues.fromByteArray(USERNAME_HASH_1)))
            .build())
        .item()).isEmpty();

    {
      final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_2);

      assertThat(maybeAccount).isPresent();
      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(),
          USERNAME_HASH_2, maybeAccount.get(), account);
    }
  }

  @Test
  void testUsernameHashConflict() {
    final Account firstAccount = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    final Account secondAccount = generateAccount("+18005559876", UUID.randomUUID(), UUID.randomUUID());

    accounts.create(firstAccount);
    accounts.create(secondAccount);

    // first account reserves and confirms username hash
    assertThatNoException().isThrownBy(() -> {
      accounts.reserveUsernameHash(firstAccount, USERNAME_HASH_1, Duration.ofDays(1));
      accounts.confirmUsernameHash(firstAccount, USERNAME_HASH_1);
    });

    final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_1);

    assertThat(maybeAccount).isPresent();
    verifyStoredState(firstAccount.getNumber(), firstAccount.getUuid(), firstAccount.getPhoneNumberIdentifier(), USERNAME_HASH_1, maybeAccount.get(), firstAccount);

    // throw an error if second account tries to reserve or confirm the same username hash
    assertThatExceptionOfType(ContestedOptimisticLockException.class)
        .isThrownBy(() -> accounts.reserveUsernameHash(secondAccount, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThatExceptionOfType(ContestedOptimisticLockException.class)
        .isThrownBy(() -> accounts.confirmUsernameHash(secondAccount, USERNAME_HASH_1));

    // throw an error if first account tries to reserve or confirm the username hash that it has already confirmed
    assertThatExceptionOfType(ContestedOptimisticLockException.class)
        .isThrownBy(() -> accounts.reserveUsernameHash(firstAccount, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThatExceptionOfType(ContestedOptimisticLockException.class)
        .isThrownBy(() -> accounts.confirmUsernameHash(firstAccount, USERNAME_HASH_1));

    assertThat(secondAccount.getReservedUsernameHash()).isEmpty();
    assertThat(secondAccount.getUsernameHash()).isEmpty();
  }

  @Test
  void testConfirmUsernameHashVersionMismatch() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);
    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    account.setVersion(account.getVersion() + 77);

    assertThatExceptionOfType(ContestedOptimisticLockException.class)
        .isThrownBy(() -> accounts.confirmUsernameHash(account, USERNAME_HASH_1));

    assertThat(account.getUsernameHash()).isEmpty();
  }

  @Test
  void testClearUsername() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1);
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1)).isPresent();

    accounts.clearUsernameHash(account);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1)).isEmpty();
    assertThat(accounts.getByAccountIdentifier(account.getUuid()))
        .hasValueSatisfying(clearedAccount -> assertThat(clearedAccount.getUsernameHash()).isEmpty());
  }

  @Test
  void testClearUsernameNoUsername() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    assertThatNoException().isThrownBy(() -> accounts.clearUsernameHash(account));
  }

  @Test
  void testClearUsernameVersionMismatch() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1);

    account.setVersion(account.getVersion() + 12);

    assertThatExceptionOfType(ContestedOptimisticLockException.class).isThrownBy(() -> accounts.clearUsernameHash(account));

    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
  }

  @Test
  void testReservedUsernameHash() {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(1));
    assertArrayEquals(account1.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(account1.getUsernameHash()).isEmpty();

    // account 2 shouldn't be able to reserve or confirm the same username hash
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.confirmUsernameHash(account2, USERNAME_HASH_1));
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1)).isEmpty();

    accounts.confirmUsernameHash(account1, USERNAME_HASH_1);
    assertThat(account1.getReservedUsernameHash()).isEmpty();
    assertArrayEquals(account1.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).get().getUuid()).isEqualTo(account1.getUuid());

    final Map<String, AttributeValue> usernameConstraintRecord = dynamoDbExtension.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(USERNAME_CONSTRAINT_TABLE_NAME)
            .key(Map.of(Accounts.ATTR_USERNAME_HASH, AttributeValues.fromByteArray(USERNAME_HASH_1)))
            .build())
        .item();

    assertThat(usernameConstraintRecord).containsKey(Accounts.ATTR_USERNAME_HASH);
    assertThat(usernameConstraintRecord).doesNotContainKey(Accounts.ATTR_TTL);
  }

  @Test
  void testUsernameHashAvailable() {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account1);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(1));
    assertThat(accounts.usernameHashAvailable(USERNAME_HASH_1)).isFalse();
    assertThat(accounts.usernameHashAvailable(Optional.empty(), USERNAME_HASH_1)).isFalse();
    assertThat(accounts.usernameHashAvailable(Optional.of(UUID.randomUUID()), USERNAME_HASH_1)).isFalse();
    assertThat(accounts.usernameHashAvailable(Optional.of(account1.getUuid()), USERNAME_HASH_1)).isTrue();

    accounts.confirmUsernameHash(account1, USERNAME_HASH_1);
    assertThat(accounts.usernameHashAvailable(USERNAME_HASH_1)).isFalse();
    assertThat(accounts.usernameHashAvailable(Optional.empty(), USERNAME_HASH_1)).isFalse();
    assertThat(accounts.usernameHashAvailable(Optional.of(UUID.randomUUID()), USERNAME_HASH_1)).isFalse();
    assertThat(accounts.usernameHashAvailable(Optional.of(account1.getUuid()), USERNAME_HASH_1)).isFalse();
  }

  @Test
  void testConfirmReservedUsernameHashWrongAccountUuid() {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(1));
    assertArrayEquals(account1.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(account1.getUsernameHash()).isEmpty();

    // only account1 should be able to confirm the reserved hash
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.confirmUsernameHash(account2, USERNAME_HASH_1));
  }

  @Test
  void testConfirmExpiredReservedUsernameHash() {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(2));

    Runnable runnable = () -> accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1));

    for (int i = 0; i <= 2; i++) {
      clock.pin(Instant.EPOCH.plus(Duration.ofDays(i)));
      assertThrows(ContestedOptimisticLockException.class, runnable::run);
    }

    // after 2 days, can reserve and confirm the hash
    clock.pin(Instant.EPOCH.plus(Duration.ofDays(2)).plus(Duration.ofSeconds(1)));
    runnable.run();
    assertEquals(account2.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);

    accounts.confirmUsernameHash(account2, USERNAME_HASH_1);

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(2)));
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.confirmUsernameHash(account1, USERNAME_HASH_1));
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).get().getUuid()).isEqualTo(account2.getUuid());
  }

  @Test
  void testRetryReserveUsernameHash() {
    final Account account = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);
    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(2));

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(2)),
        "Shouldn't be able to re-reserve same username hash (would extend ttl)");
  }

  @Test
  void testReserveConfirmUsernameHashVersionConflict() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);
    account.setVersion(account.getVersion() + 12);
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.confirmUsernameHash(account, USERNAME_HASH_1));
    assertThat(account.getReservedUsernameHash()).isEmpty();
    assertThat(account.getUsernameHash()).isEmpty();
  }

  private Device generateDevice(long id) {
    return DevicesHelper.createDevice(id);
  }

  private Account generateAccount(String number, UUID uuid, final UUID pni) {
    Device device = generateDevice(1);
    return generateAccount(number, uuid, pni, List.of(device));
  }

  private Account generateAccount(String number, UUID uuid, final UUID pni, List<Device> devices) {
    byte[]       unidentifiedAccessKey = new byte[16];
    Random random = new Random(System.currentTimeMillis());
    Arrays.fill(unidentifiedAccessKey, (byte)random.nextInt(255));

    return AccountsHelper.generateTestAccount(number, uuid, pni, devices, unidentifiedAccessKey);
  }

  private void assertPhoneNumberConstraintExists(final String number, final UUID uuid) {
    final GetItemResponse numberConstraintResponse = dynamoDbExtension.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(NUMBER_CONSTRAINT_TABLE_NAME)
            .key(Map.of(Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(number)))
            .build());

    assertThat(numberConstraintResponse.hasItem()).isTrue();
    assertThat(AttributeValues.getUUID(numberConstraintResponse.item(), Accounts.KEY_ACCOUNT_UUID, null)).isEqualTo(uuid);
  }

  private void assertPhoneNumberConstraintDoesNotExist(final String number) {
    final GetItemResponse numberConstraintResponse = dynamoDbExtension.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(NUMBER_CONSTRAINT_TABLE_NAME)
            .key(Map.of(Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(number)))
            .build());

    assertThat(numberConstraintResponse.hasItem()).isFalse();
  }

  private void assertPhoneNumberIdentifierConstraintExists(final UUID phoneNumberIdentifier, final UUID uuid) {
    final GetItemResponse pniConstraintResponse = dynamoDbExtension.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(PNI_CONSTRAINT_TABLE_NAME)
            .key(Map.of(Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier)))
            .build());

    assertThat(pniConstraintResponse.hasItem()).isTrue();
    assertThat(AttributeValues.getUUID(pniConstraintResponse.item(), Accounts.KEY_ACCOUNT_UUID, null)).isEqualTo(uuid);
  }

  private void assertPhoneNumberIdentifierConstraintDoesNotExist(final UUID phoneNumberIdentifier) {
    final GetItemResponse pniConstraintResponse = dynamoDbExtension.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(PNI_CONSTRAINT_TABLE_NAME)
            .key(Map.of(Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier)))
            .build());

    assertThat(pniConstraintResponse.hasItem()).isFalse();
  }

  private void verifyStoredState(String number, UUID uuid, UUID pni, byte[] usernameHash, Account expecting, boolean canonicallyDiscoverable) {
    final DynamoDbClient db = dynamoDbExtension.getDynamoDbClient();

    final GetItemResponse get = db.getItem(GetItemRequest.builder()
        .tableName(dynamoDbExtension.getTableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid)))
        .consistentRead(true)
        .build());

    if (get.hasItem()) {
      String data = new String(get.item().get(Accounts.ATTR_ACCOUNT_DATA).b().asByteArray(), StandardCharsets.UTF_8);
      assertThat(data).isNotEmpty();

      assertThat(AttributeValues.getInt(get.item(), Accounts.ATTR_VERSION, -1))
          .isEqualTo(expecting.getVersion());

      assertThat(AttributeValues.getBool(get.item(), Accounts.ATTR_CANONICALLY_DISCOVERABLE,
          !canonicallyDiscoverable)).isEqualTo(canonicallyDiscoverable);

      assertThat(AttributeValues.getByteArray(get.item(), Accounts.ATTR_UAK, null))
          .isEqualTo(expecting.getUnidentifiedAccessKey().orElse(null));

      assertArrayEquals(AttributeValues.getByteArray(get.item(), Accounts.ATTR_USERNAME_HASH, null), usernameHash);

      Account result = Accounts.fromItem(get.item());
      verifyStoredState(number, uuid, pni, usernameHash, result, expecting);
    } else {
      throw new AssertionError("No data");
    }
  }

  private void verifyStoredState(String number, UUID uuid, UUID pni, byte[] usernameHash, Account result, Account expecting) {
    assertThat(result.getNumber()).isEqualTo(number);
    assertThat(result.getPhoneNumberIdentifier()).isEqualTo(pni);
    assertThat(result.getLastSeen()).isEqualTo(expecting.getLastSeen());
    assertThat(result.getUuid()).isEqualTo(uuid);
    assertThat(result.getVersion()).isEqualTo(expecting.getVersion());
    assertArrayEquals(result.getUsernameHash().orElse(null), usernameHash);
    assertThat(Arrays.equals(result.getUnidentifiedAccessKey().get(), expecting.getUnidentifiedAccessKey().get())).isTrue();

    for (Device expectingDevice : expecting.getDevices()) {
      Device resultDevice = result.getDevice(expectingDevice.getId()).get();
      assertThat(resultDevice.getApnId()).isEqualTo(expectingDevice.getApnId());
      assertThat(resultDevice.getGcmId()).isEqualTo(expectingDevice.getGcmId());
      assertThat(resultDevice.getLastSeen()).isEqualTo(expectingDevice.getLastSeen());
      assertThat(resultDevice.getSignedPreKey().getPublicKey()).isEqualTo(expectingDevice.getSignedPreKey().getPublicKey());
      assertThat(resultDevice.getSignedPreKey().getKeyId()).isEqualTo(expectingDevice.getSignedPreKey().getKeyId());
      assertThat(resultDevice.getSignedPreKey().getSignature()).isEqualTo(expectingDevice.getSignedPreKey().getSignature());
      assertThat(resultDevice.getFetchesMessages()).isEqualTo(expectingDevice.getFetchesMessages());
      assertThat(resultDevice.getUserAgent()).isEqualTo(expectingDevice.getUserAgent());
      assertThat(resultDevice.getName()).isEqualTo(expectingDevice.getName());
      assertThat(resultDevice.getCreated()).isEqualTo(expectingDevice.getCreated());
    }
  }
}
