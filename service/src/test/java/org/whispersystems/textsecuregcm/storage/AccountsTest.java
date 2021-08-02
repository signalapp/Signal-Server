/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.AssertionsForClassTypes;
import org.jdbi.v3.core.transaction.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

class AccountsTest {

  private static final String ACCOUNTS_TABLE_NAME = "accounts_test";
  private static final String NUMBERS_TABLE_NAME = "numbers_test";

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

  private Accounts accounts;

  @BeforeEach
  void setupAccountsDao() {
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

    dynamoDbExtension.getDynamoDbClient().createTable(createNumbersTableRequest);

    this.accounts = new Accounts(
        dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getTableName(),
        NUMBERS_TABLE_NAME,
        SCAN_PAGE_SIZE);
  }

  @Test
  void testStore() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(device));

    boolean freshUser = accounts.create(account);

    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account, true);

    freshUser = accounts.create(account);
    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account, true);

  }

  @Test
  void testStoreMulti() {
    Set<Device> devices = new HashSet<>();
    devices.add(generateDevice(1));
    devices.add(generateDevice(2));

    Account account = generateAccount("+14151112222", UUID.randomUUID(), devices);

    accounts.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account, true);
  }

  @Test
  void testRetrieve() {
    Set<Device> devicesFirst = new HashSet<>();
    devicesFirst.add(generateDevice(1));
    devicesFirst.add(generateDevice(2));

    UUID uuidFirst = UUID.randomUUID();
    Account accountFirst = generateAccount("+14151112222", uuidFirst, devicesFirst);

    Set<Device> devicesSecond = new HashSet<>();
    devicesSecond.add(generateDevice(1));
    devicesSecond.add(generateDevice(2));

    UUID uuidSecond = UUID.randomUUID();
    Account accountSecond = generateAccount("+14152221111", uuidSecond, devicesSecond);

    accounts.create(accountFirst);
    accounts.create(accountSecond);

    Optional<Account> retrievedFirst = accounts.get("+14151112222");
    Optional<Account> retrievedSecond = accounts.get("+14152221111");

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.get(uuidFirst);
    retrievedSecond = accounts.get(uuidSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, retrievedSecond.get(), accountSecond);
  }

  @Test
  void testOverwrite() {
    Device  device  = generateDevice (1                                            );
    UUID    firstUuid = UUID.randomUUID();
    Account account   = generateAccount("+14151112222", firstUuid, Collections.singleton(device));

    accounts.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account, true);

    account.setProfileName("name");

    accounts.update(account);

    UUID secondUuid = UUID.randomUUID();

    device = generateDevice(1);
    account = generateAccount("+14151112222", secondUuid, Collections.singleton(device));

    final boolean freshUser = accounts.create(account);
    assertThat(freshUser).isFalse();
    verifyStoredState("+14151112222", firstUuid, account, true);

    device = generateDevice(1);
    Account invalidAccount = generateAccount("+14151113333", firstUuid, Collections.singleton(device));

    assertThatThrownBy(() -> accounts.create(invalidAccount));
  }

  @Test
  void testUpdate() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(device));

    accounts.create(account);

    device.setName("foobar");

    accounts.update(account);

    Optional<Account> retrieved = accounts.get("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), retrieved.get(), account);

    retrieved = accounts.get(account.getUuid());

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account, true);

    device = generateDevice(1);
    Account unknownAccount = generateAccount("+14151113333", UUID.randomUUID(), Collections.singleton(device));

    assertThatThrownBy(() -> accounts.update(unknownAccount)).isInstanceOfAny(ConditionalCheckFailedException.class);

    account.setProfileName("name");

    accounts.update(account);

    assertThat(account.getVersion()).isEqualTo(2);

    verifyStoredState("+14151112222", account.getUuid(), account, true);

    account.setVersion(1);

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);

    account.setVersion(2);
    account.setProfileName("name2");

    accounts.update(account);

    verifyStoredState("+14151112222", account.getUuid(), account, true);
  }

  @Test
  void testUpdateWithMockTransactionConflictException() {

    final DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
    accounts = new Accounts(dynamoDbClient,
        dynamoDbExtension.getTableName(), NUMBERS_TABLE_NAME, SCAN_PAGE_SIZE);

    when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
        .thenThrow(TransactionConflictException.class);

    Device device = generateDevice(1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(device));

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);
  }

  @Test
  void testRetrieveFrom() {
    List<Account> users = new ArrayList<>();

    for (int i = 1; i <= 100; i++) {
      Account account = generateAccount("+1" + String.format("%03d", i), UUID.randomUUID());
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

      verifyStoredState(expectedAccount.getNumber(), expectedAccount.getUuid(), retrievedAccount, expectedAccount);

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

        verifyStoredState(expectedAccount.getNumber(), expectedAccount.getUuid(), retrievedAccount, expectedAccount);

        users.remove(expectedAccount);
      }
    }

    assertThat(users).isEmpty();
  }

  @Test
  void testDelete() {
    final Device deletedDevice = generateDevice(1);
    final Account deletedAccount = generateAccount("+14151112222", UUID.randomUUID(),
        Collections.singleton(deletedDevice));
    final Device retainedDevice = generateDevice(1);
    final Account retainedAccount = generateAccount("+14151112345", UUID.randomUUID(),
        Collections.singleton(retainedDevice));

    accounts.create(deletedAccount);
    accounts.create(retainedAccount);

    assertThat(accounts.get(deletedAccount.getUuid())).isPresent();
    assertThat(accounts.get(retainedAccount.getUuid())).isPresent();

    accounts.delete(deletedAccount.getUuid());

    assertThat(accounts.get(deletedAccount.getUuid())).isNotPresent();

    verifyStoredState(retainedAccount.getNumber(), retainedAccount.getUuid(),
        accounts.get(retainedAccount.getUuid()).get(), retainedAccount);

    {
      final Account recreatedAccount = generateAccount(deletedAccount.getNumber(), UUID.randomUUID(),
          Collections.singleton(generateDevice(1)));

      final boolean freshUser = accounts.create(recreatedAccount);

      assertThat(freshUser).isTrue();
      assertThat(accounts.get(recreatedAccount.getUuid())).isPresent();
      verifyStoredState(recreatedAccount.getNumber(), recreatedAccount.getUuid(),
          accounts.get(recreatedAccount.getUuid()).get(), recreatedAccount);
    }
  }

  @Test
  void testMissing() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(device));

    accounts.create(account);

    Optional<Account> retrieved = accounts.get("+11111111");
    assertThat(retrieved.isPresent()).isFalse();

    retrieved = accounts.get(UUID.randomUUID());
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

    when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(RuntimeException.class);

    when(client.updateItem(any(UpdateItemRequest.class)))
        .thenThrow(RuntimeException.class);

    Accounts accounts = new Accounts(client, ACCOUNTS_TABLE_NAME, NUMBERS_TABLE_NAME, SCAN_PAGE_SIZE);
    Account account = generateAccount("+14151112222", UUID.randomUUID());

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
    UUID uuid = UUID.randomUUID();
    Account account = generateAccount("+14151112222", uuid, Collections.singleton(device));
    account.setDiscoverableByPhoneNumber(false);
    accounts.create(account);
    verifyStoredState("+14151112222", account.getUuid(), account, false);
    account.setDiscoverableByPhoneNumber(true);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account, true);
    account.setDiscoverableByPhoneNumber(false);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account, false);
  }

  @Test
  public void testChangeNumber() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final Device device = generateDevice(1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), Collections.singleton(device));

    accounts.create(account);

    {
      final Optional<Account> retrieved = accounts.get(originalNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(originalNumber, account.getUuid(), retrieved.get(), account);
    }

    accounts.changeNumber(account, targetNumber);

    assertThat(accounts.get(originalNumber)).isEmpty();

    {
      final Optional<Account> retrieved = accounts.get(targetNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(targetNumber, account.getUuid(), retrieved.get(), account);
    }
  }

  @Test
  public void testChangeNumberConflict() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final Device existingDevice = generateDevice(1);
    final Account existingAccount = generateAccount(targetNumber, UUID.randomUUID(), Collections.singleton(existingDevice));

    final Device device = generateDevice(1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), Collections.singleton(device));

    accounts.create(account);
    accounts.create(existingAccount);

    assertThrows(TransactionCanceledException.class, () -> accounts.changeNumber(account, targetNumber));
  }

  private Device generateDevice(long id) {
    Random       random       = new Random(System.currentTimeMillis());
    SignedPreKey signedPreKey = new SignedPreKey(random.nextInt(), "testPublicKey-" + random.nextInt(), "testSignature-" + random.nextInt());
    return new Device(id, "testName-" + random.nextInt(), "testAuthToken-" + random.nextInt(), "testSalt-" + random.nextInt(),
        "testGcmId-" + random.nextInt(), "testApnId-" + random.nextInt(), "testVoipApnId-" + random.nextInt(), random.nextBoolean(), random.nextInt(), signedPreKey, random.nextInt(), random.nextInt(), "testUserAgent-" + random.nextInt() , 0, new Device.DeviceCapabilities(random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(),
        random.nextBoolean(), random.nextBoolean(), random.nextBoolean()));
  }

  private Account generateAccount(String number, UUID uuid) {
    Device device = generateDevice(1);
    return generateAccount(number, uuid, Collections.singleton(device));
  }

  private Account generateAccount(String number, UUID uuid, Set<Device> devices) {
    byte[]       unidentifiedAccessKey = new byte[16];
    Random random = new Random(System.currentTimeMillis());
    Arrays.fill(unidentifiedAccessKey, (byte)random.nextInt(255));

    return new Account(number, uuid, devices, unidentifiedAccessKey);
  }

  private void verifyStoredState(String number, UUID uuid, Account expecting, boolean canonicallyDiscoverable) {
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

      Account result = Accounts.fromItem(get.item());
      verifyStoredState(number, uuid, result, expecting);
    } else {
      throw new AssertionError("No data");
    }
  }

  private void verifyStoredState(String number, UUID uuid, Account result, Account expecting) {
    assertThat(result.getNumber()).isEqualTo(number);
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
