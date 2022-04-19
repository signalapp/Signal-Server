/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.uuid.UUIDComparator;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jdbi.v3.core.transaction.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.SystemMapper;
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
            .attributeName(Accounts.ATTR_USERNAME)
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(Accounts.ATTR_USERNAME)
            .attributeType(ScalarAttributeType.S)
            .build())
        .provisionedThroughput(DynamoDbExtension.DEFAULT_PROVISIONED_THROUGHPUT)
        .build();

    dynamoDbExtension.getDynamoDbClient().createTable(createUsernamesTableRequest);

    @SuppressWarnings("unchecked") DynamicConfigurationManager<DynamicConfiguration> m = mock(DynamicConfigurationManager.class);
    mockDynamicConfigManager = m;

    when(mockDynamicConfigManager.getConfiguration())
        .thenReturn(new DynamicConfiguration());

    this.accounts = new Accounts(
        mockDynamicConfigManager,
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
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), Collections.singleton(device));

    boolean freshUser = accounts.create(account);

    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    freshUser = accounts.create(account);
    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());
  }

  @Test
  void testStoreMulti() {
    Set<Device> devices = new HashSet<>();
    devices.add(generateDevice(1));
    devices.add(generateDevice(2));

    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), devices);

    accounts.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());
  }

  @Test
  void testRetrieve() {
    Set<Device> devicesFirst = new HashSet<>();
    devicesFirst.add(generateDevice(1));
    devicesFirst.add(generateDevice(2));

    UUID uuidFirst = UUID.randomUUID();
    UUID pniFirst = UUID.randomUUID();
    Account accountFirst = generateAccount("+14151112222", uuidFirst, pniFirst, devicesFirst);

    Set<Device> devicesSecond = new HashSet<>();
    devicesSecond.add(generateDevice(1));
    devicesSecond.add(generateDevice(2));

    UUID uuidSecond = UUID.randomUUID();
    UUID pniSecond = UUID.randomUUID();
    Account accountSecond = generateAccount("+14152221111", uuidSecond, pniSecond, devicesSecond);

    accounts.create(accountFirst);
    accounts.create(accountSecond);

    Optional<Account> retrievedFirst = accounts.getByE164("+14151112222");
    Optional<Account> retrievedSecond = accounts.getByE164("+14152221111");

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByAccountIdentifier(uuidFirst);
    retrievedSecond = accounts.getByAccountIdentifier(uuidSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByPhoneNumberIdentifier(pniFirst);
    retrievedSecond = accounts.getByPhoneNumberIdentifier(pniSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, retrievedSecond.get(), accountSecond);
  }

  @Test
  void testRetrieveNoPni() throws JsonProcessingException {
    final Set<Device> devices = new HashSet<>();
    devices.add(generateDevice(1));
    devices.add(generateDevice(2));

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
                  Accounts.ATTR_ACCOUNT_DATA, AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
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
    verifyStoredState("+14151112222", uuid, null, retrieved.get(), account);

    retrieved = accounts.getByAccountIdentifier(uuid);

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", uuid, null, retrieved.get(), account);
  }

  @Test
  void testOverwrite() {
    Device device = generateDevice(1);
    UUID firstUuid = UUID.randomUUID();
    UUID firstPni = UUID.randomUUID();
    Account account = generateAccount("+14151112222", firstUuid, firstPni, Collections.singleton(device));

    accounts.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);

    assertPhoneNumberConstraintExists("+14151112222", firstUuid);
    assertPhoneNumberIdentifierConstraintExists(firstPni, firstUuid);

    accounts.update(account);

    UUID secondUuid = UUID.randomUUID();

    device = generateDevice(1);
    account = generateAccount("+14151112222", secondUuid, UUID.randomUUID(), Collections.singleton(device));

    final boolean freshUser = accounts.create(account);
    assertThat(freshUser).isFalse();
    verifyStoredState("+14151112222", firstUuid, firstPni, account, true);

    assertPhoneNumberConstraintExists("+14151112222", firstUuid);
    assertPhoneNumberIdentifierConstraintExists(firstPni, firstUuid);

    device = generateDevice(1);
    Account invalidAccount = generateAccount("+14151113333", firstUuid, UUID.randomUUID(), Collections.singleton(device));

    assertThatThrownBy(() -> accounts.create(invalidAccount));
  }

  @Test
  void testUpdate() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), Collections.singleton(device));

    accounts.create(account);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    device.setName("foobar");

    accounts.update(account);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    Optional<Account> retrieved = accounts.getByE164("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), retrieved.get(), account);

    retrieved = accounts.getByAccountIdentifier(account.getUuid());

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);

    device = generateDevice(1);
    Account unknownAccount = generateAccount("+14151113333", UUID.randomUUID(), UUID.randomUUID(), Collections.singleton(device));

    assertThatThrownBy(() -> accounts.update(unknownAccount)).isInstanceOfAny(ConditionalCheckFailedException.class);

    accounts.update(account);

    assertThat(account.getVersion()).isEqualTo(2);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);

    account.setVersion(1);

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);

    account.setVersion(2);

    accounts.update(account);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdateWithMockTransactionConflictException(boolean wrapException) {

    final DynamoDbAsyncClient dynamoDbAsyncClient = mock(DynamoDbAsyncClient.class);
    accounts = new Accounts(mockDynamicConfigManager, mock(DynamoDbClient.class),
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

      verifyStoredState(expectedAccount.getNumber(), expectedAccount.getUuid(), expectedAccount.getPhoneNumberIdentifier(), retrievedAccount, expectedAccount);

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

        verifyStoredState(expectedAccount.getNumber(), expectedAccount.getUuid(), expectedAccount.getPhoneNumberIdentifier(), retrievedAccount, expectedAccount);

        users.remove(expectedAccount);
      }
    }

    assertThat(users).isEmpty();
  }

  @Test
  void testDelete() {
    final Device deletedDevice = generateDevice(1);
    final Account deletedAccount = generateAccount("+14151112222", UUID.randomUUID(),
        UUID.randomUUID(), Collections.singleton(deletedDevice));
    final Device retainedDevice = generateDevice(1);
    final Account retainedAccount = generateAccount("+14151112345", UUID.randomUUID(),
        UUID.randomUUID(), Collections.singleton(retainedDevice));

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
        accounts.getByAccountIdentifier(retainedAccount.getUuid()).get(), retainedAccount);

    {
      final Account recreatedAccount = generateAccount(deletedAccount.getNumber(), UUID.randomUUID(),
          UUID.randomUUID(), Collections.singleton(generateDevice(1)));

      final boolean freshUser = accounts.create(recreatedAccount);

      assertThat(freshUser).isTrue();
      assertThat(accounts.getByAccountIdentifier(recreatedAccount.getUuid())).isPresent();
      verifyStoredState(recreatedAccount.getNumber(), recreatedAccount.getUuid(), recreatedAccount.getPhoneNumberIdentifier(),
          accounts.getByAccountIdentifier(recreatedAccount.getUuid()).get(), recreatedAccount);

      assertPhoneNumberConstraintExists(recreatedAccount.getNumber(), recreatedAccount.getUuid());
      assertPhoneNumberIdentifierConstraintExists(recreatedAccount.getPhoneNumberIdentifier(), recreatedAccount.getUuid());
    }
  }

  @Test
  void testMissing() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), Collections.singleton(device));

    accounts.create(account);

    Optional<Account> retrieved = accounts.getByE164("+11111111");
    assertThat(retrieved.isPresent()).isFalse();

    retrieved = accounts.getByAccountIdentifier(UUID.randomUUID());
    assertThat(retrieved.isPresent()).isFalse();
  }

  @Test
  @Disabled("Need fault tolerant dynamodb")
  void testBreaker() throws InterruptedException {

    CircuitBreakerConfiguration configuration = new CircuitBreakerConfiguration();
    configuration.setWaitDurationInOpenStateInSeconds(1);
    configuration.setRingBufferSizeInHalfOpenState(1);
    configuration.setRingBufferSizeInClosedState(2);
    configuration.setFailureRateThreshold(50);

    final DynamoDbClient client = mock(DynamoDbClient.class);
    final DynamoDbAsyncClient asyncClient = mock(DynamoDbAsyncClient.class);

    when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(RuntimeException.class);

    when(asyncClient.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    Accounts accounts = new Accounts(mockDynamicConfigManager, client, asyncClient, ACCOUNTS_TABLE_NAME, NUMBER_CONSTRAINT_TABLE_NAME,
        PNI_CONSTRAINT_TABLE_NAME, USERNAME_CONSTRAINT_TABLE_NAME, SCAN_PAGE_SIZE);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID());

    try {
      accounts.update(account);
      throw new AssertionError();
    } catch (TransactionException e) {
      // good
    }

    try {
      accounts.update(account);
      throw new AssertionError();
    } catch (TransactionException e) {
      // good
    }

    try {
      accounts.update(account);
      throw new AssertionError();
    } catch (CallNotPermittedException e) {
      // good
    }

    Thread.sleep(1100);

    try {
      accounts.update(account);
      throw new AssertionError();
    } catch (TransactionException e) {
      // good
    }
  }

  @Test
  void testCanonicallyDiscoverableSet() {
    Device device = generateDevice(1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), Collections.singleton(device));
    account.setDiscoverableByPhoneNumber(false);
    accounts.create(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, false);
    account.setDiscoverableByPhoneNumber(true);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, true);
    account.setDiscoverableByPhoneNumber(false);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), account, false);
  }

  @Test
  public void testChangeNumber() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Device device = generateDevice(1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), originalPni, Collections.singleton(device));

    accounts.create(account);

    assertThat(accounts.getByPhoneNumberIdentifier(originalPni)).isPresent();

    assertPhoneNumberConstraintExists(originalNumber, account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(originalPni, account.getUuid());

    {
      final Optional<Account> retrieved = accounts.getByE164(originalNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(originalNumber, account.getUuid(), account.getPhoneNumberIdentifier(), retrieved.get(), account);
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

      verifyStoredState(targetNumber, account.getUuid(), account.getPhoneNumberIdentifier(), retrieved.get(), account);

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
    final Account existingAccount = generateAccount(targetNumber, UUID.randomUUID(), targetPni, Collections.singleton(existingDevice));

    final Device device = generateDevice(1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), originalPni, Collections.singleton(device));

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
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), UUID.randomUUID(), Collections.singleton(device));

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
  void testSetUsername() throws UsernameNotAvailableException {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    final String username = "test";

    assertThat(accounts.getByUsername(username)).isEmpty();

    accounts.setUsername(account, username);

    {
      final Optional<Account> maybeAccount = accounts.getByUsername(username);

      assertThat(maybeAccount).hasValueSatisfying(retrievedAccount ->
          assertThat(retrievedAccount.getUsername()).hasValueSatisfying(retrievedUsername ->
              assertThat(retrievedUsername).isEqualTo(username)));

      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(),          maybeAccount.orElseThrow(), account);
    }

    final String secondUsername = username + "2";

    accounts.setUsername(account, secondUsername);

    assertThat(accounts.getByUsername(username)).isEmpty();

    {
      final Optional<Account> maybeAccount = accounts.getByUsername(secondUsername);

      assertThat(maybeAccount).isPresent();
      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(),
          maybeAccount.get(), account);
    }
  }

  @Test
  void testSetUsernameConflict() {
    final Account firstAccount = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    final Account secondAccount = generateAccount("+18005559876", UUID.randomUUID(), UUID.randomUUID());

    accounts.create(firstAccount);
    accounts.create(secondAccount);

    final String username = "test";

    assertThatNoException().isThrownBy(() -> accounts.setUsername(firstAccount, username));

    final Optional<Account> maybeAccount = accounts.getByUsername(username);

    assertThat(maybeAccount).isPresent();
    verifyStoredState(firstAccount.getNumber(), firstAccount.getUuid(), firstAccount.getPhoneNumberIdentifier(), maybeAccount.get(), firstAccount);

    assertThatExceptionOfType(UsernameNotAvailableException.class)
        .isThrownBy(() -> accounts.setUsername(secondAccount, username));

    assertThat(secondAccount.getUsername()).isEmpty();
  }

  @Test
  void testSetUsernameVersionMismatch() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);
    account.setVersion(account.getVersion() + 77);

    assertThatExceptionOfType(ContestedOptimisticLockException.class)
        .isThrownBy(() -> accounts.setUsername(account, "test"));

    assertThat(account.getUsername()).isEmpty();
  }

  @Test
  void testClearUsername() throws UsernameNotAvailableException {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    final String username = "test";

    accounts.setUsername(account, username);
    assertThat(accounts.getByUsername(username)).isPresent();

    accounts.clearUsername(account);

    assertThat(accounts.getByUsername(username)).isEmpty();
    assertThat(accounts.getByAccountIdentifier(account.getUuid()))
        .hasValueSatisfying(clearedAccount -> assertThat(clearedAccount.getUsername()).isEmpty());
  }

  @Test
  void testClearUsernameNoUsername() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    assertThatNoException().isThrownBy(() -> accounts.clearUsername(account));
  }

  @Test
  void testClearUsernameVersionMismatch() throws UsernameNotAvailableException {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    accounts.create(account);

    final String username = "test";

    accounts.setUsername(account, username);

    account.setVersion(account.getVersion() + 12);

    assertThatExceptionOfType(ContestedOptimisticLockException.class).isThrownBy(() -> accounts.clearUsername(account));

    assertThat(account.getUsername()).hasValueSatisfying(u -> assertThat(u).isEqualTo(username));
  }

  @Test
  void testAddUakMissingInJson() {
      // If there's no uak in the json, we shouldn't add an attribute on crawl
    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = generateAccount("+18005551234", accountIdentifier, UUID.randomUUID());
    account.setUnidentifiedAccessKey(null);
    accounts.create(account);

    // there should be no top level uak
    Map<String, AttributeValue> item = dynamoDbExtension.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(ACCOUNTS_TABLE_NAME)
            .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier)))
            .consistentRead(true)
            .build()).item();
    assertThat(item).doesNotContainKey(Accounts.ATTR_UAK);

    // crawling should return 1 account
    final AccountCrawlChunk allFromStart = accounts.getAllFromStart(1);
    assertThat(allFromStart.getAccounts()).hasSize(1);
    assertThat(allFromStart.getAccounts().get(0).getUuid()).isEqualTo(accountIdentifier);

    // there should still be no top level uak
    item = dynamoDbExtension.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(ACCOUNTS_TABLE_NAME)
            .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier)))
            .consistentRead(true)
            .build()).item();
    assertThat(item).doesNotContainKey(Accounts.ATTR_UAK);
  }

  @Test
  void testUakMismatch() {
    // If there's a UAK mismatch, we should correct it
    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = generateAccount("+18005551234", accountIdentifier, UUID.randomUUID());
    accounts.create(account);

    // set the uak to garbage in the attributes
    dynamoDbExtension.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
        .tableName(ACCOUNTS_TABLE_NAME)
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier)))
        .expressionAttributeNames(Map.of("#uak", Accounts.ATTR_UAK))
        .expressionAttributeValues(Map.of(":uak", AttributeValues.fromByteArray("bad-uak".getBytes())))
        .updateExpression("SET #uak = :uak").build());

    // crawling should return 1 account and fix the uak mismatch
    final AccountCrawlChunk allFromStart = accounts.getAllFromStart(1);
    assertThat(allFromStart.getAccounts()).hasSize(1);
    assertThat(allFromStart.getAccounts().get(0).getUuid()).isEqualTo(accountIdentifier);
    assertThat(allFromStart.getAccounts().get(0).getUnidentifiedAccessKey().get()).isEqualTo(account.getUnidentifiedAccessKey().get());

    // the top level uak should be the original
    final Map<String, AttributeValue> item = dynamoDbExtension.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(ACCOUNTS_TABLE_NAME)
            .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier)))
            .consistentRead(true)
            .build()).item();
    assertThat(item).containsEntry(
        Accounts.ATTR_UAK,
        AttributeValues.fromByteArray(account.getUnidentifiedAccessKey().get()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testAddMissingUakAttribute(boolean normalizeDisabled) throws JsonProcessingException {
    final UUID accountIdentifier = UUID.randomUUID();

    if (normalizeDisabled) {
      final DynamicConfiguration config = DynamicConfigurationManager.parseConfiguration("""
          captcha:
            scoreFloor: 1.0
          uakMigrationConfiguration:
            enabled: false
          """, DynamicConfiguration.class).orElseThrow();
      when(mockDynamicConfigManager.getConfiguration()).thenReturn(config);
    }

    final Account account = generateAccount("+18005551234", accountIdentifier, UUID.randomUUID());
    accounts.create(account);

    // remove the top level uak (simulates old format)
    dynamoDbExtension.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
        .tableName(ACCOUNTS_TABLE_NAME)
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier)))
        .expressionAttributeNames(Map.of("#uak", Accounts.ATTR_UAK))
        .updateExpression("REMOVE #uak").build());

    // crawling should return 1 account, and fix the discrepancy between
    // the json blob and the top level attributes if normalization is enabled
    final AccountCrawlChunk allFromStart = accounts.getAllFromStart(1);
    assertThat(allFromStart.getAccounts()).hasSize(1);
    assertThat(allFromStart.getAccounts().get(0).getUuid()).isEqualTo(accountIdentifier);

    // check whether normalization happened
    final Map<String, AttributeValue> item = dynamoDbExtension.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(ACCOUNTS_TABLE_NAME)
            .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier)))
            .consistentRead(true)
            .build()).item();
    if (normalizeDisabled) {
      assertThat(item).doesNotContainKey(Accounts.ATTR_UAK);
    } else {
      assertThat(item).containsEntry(Accounts.ATTR_UAK,
          AttributeValues.fromByteArray(account.getUnidentifiedAccessKey().get()));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {24, 25, 26, 101})
  void testAddMissingUakAttributeBatched(int n) {
    // generate N + 5 accounts
    List<Account> allAccounts = IntStream.range(0, n + 5)
        .mapToObj(i -> generateAccount(String.format("+1800555%04d", i), UUID.randomUUID(), UUID.randomUUID()))
        .collect(Collectors.toList());
    allAccounts.forEach(accounts::create);

    // delete the UAK on n of them
    Collections.shuffle(allAccounts);
    allAccounts.stream().limit(n).forEach(account ->
      dynamoDbExtension.getDynamoDbClient().updateItem(UpdateItemRequest.builder()
          .tableName(ACCOUNTS_TABLE_NAME)
          .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
          .expressionAttributeNames(Map.of("#uak", Accounts.ATTR_UAK))
          .updateExpression("REMOVE #uak")
          .build()));

    // crawling should fix the discrepancy between
    // the json blob and the top level attributes
    AccountCrawlChunk chunk = accounts.getAllFromStart(7);
    long verifiedCount = 0;
    while (true) {
      for (Account account : chunk.getAccounts()) {
        // check that the attribute now exists at top level
        final Map<String, AttributeValue> item = dynamoDbExtension.getDynamoDbClient()
            .getItem(GetItemRequest.builder()
                .tableName(ACCOUNTS_TABLE_NAME)
                .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
                .consistentRead(true)
                .build()).item();
        assertThat(item).containsEntry(Accounts.ATTR_UAK,
            AttributeValues.fromByteArray(account.getUnidentifiedAccessKey().get()));
        verifiedCount++;
      }
      if (chunk.getLastUuid().isPresent()) {
        chunk = accounts.getAllFrom(chunk.getLastUuid().get(), 7);
      } else {
        break;
      }
    }
    assertThat(verifiedCount).isEqualTo(n + 5);
  }

  private Device generateDevice(long id) {
    Random random = new Random(System.currentTimeMillis());
    SignedPreKey signedPreKey = new SignedPreKey(random.nextInt(), "testPublicKey-" + random.nextInt(),
        "testSignature-" + random.nextInt());
    return new Device(id, "testName-" + random.nextInt(), "testAuthToken-" + random.nextInt(),
        "testSalt-" + random.nextInt(),
        "testGcmId-" + random.nextInt(), "testApnId-" + random.nextInt(), "testVoipApnId-" + random.nextInt(),
        random.nextBoolean(), random.nextInt(), signedPreKey, random.nextInt(), random.nextInt(),
        "testUserAgent-" + random.nextInt(), 0,
        new Device.DeviceCapabilities(random.nextBoolean(), random.nextBoolean(), random.nextBoolean(),
            random.nextBoolean(), random.nextBoolean(), random.nextBoolean(),
            random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(),
            random.nextBoolean(), random.nextBoolean()));
  }

  private Account generateAccount(String number, UUID uuid, final UUID pni) {
    Device device = generateDevice(1);
    return generateAccount(number, uuid, pni, Collections.singleton(device));
  }

  private Account generateAccount(String number, UUID uuid, final UUID pni, Set<Device> devices) {
    byte[]       unidentifiedAccessKey = new byte[16];
    Random random = new Random(System.currentTimeMillis());
    Arrays.fill(unidentifiedAccessKey, (byte)random.nextInt(255));

    return new Account(number, uuid, pni, devices, unidentifiedAccessKey);
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

  private void verifyStoredState(String number, UUID uuid, UUID pni, Account expecting, boolean canonicallyDiscoverable) {
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

      Account result = Accounts.fromItem(get.item());
      verifyStoredState(number, uuid, pni, result, expecting);
    } else {
      throw new AssertionError("No data");
    }
  }

  private void verifyStoredState(String number, UUID uuid, UUID pni, Account result, Account expecting) {
    assertThat(result.getNumber()).isEqualTo(number);
    assertThat(result.getPhoneNumberIdentifier()).isEqualTo(pni);
    assertThat(result.getLastSeen()).isEqualTo(expecting.getLastSeen());
    assertThat(result.getUuid()).isEqualTo(uuid);
    assertThat(result.getVersion()).isEqualTo(expecting.getVersion());
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
