/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jdbi.v3.core.transaction.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class AccountsDynamoDbTest {

  private static final String ACCOUNTS_TABLE_NAME = "accounts_test";
  private static final String NUMBERS_TABLE_NAME = "numbers_test";
  private static final String MIGRATION_DELETED_ACCOUNTS_TABLE_NAME = "migration_deleted_accounts_test";
  private static final String MIGRATION_RETRY_ACCOUNTS_TABLE_NAME = "miration_retry_accounts_test";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(ACCOUNTS_TABLE_NAME)
      .hashKey(AccountsDynamoDb.KEY_ACCOUNT_UUID)
      .attributeDefinition(new AttributeDefinition(AccountsDynamoDb.KEY_ACCOUNT_UUID, ScalarAttributeType.B))
      .build();

  private AccountsDynamoDb accountsDynamoDb;

  private Table migrationDeletedAccountsTable;
  private Table migrationRetryAccountsTable;

  @BeforeEach
  void setupAccountsDao() {

    CreateTableRequest createNumbersTableRequest = new CreateTableRequest()
        .withTableName(NUMBERS_TABLE_NAME)
        .withKeySchema(new KeySchemaElement(AccountsDynamoDb.ATTR_ACCOUNT_E164, KeyType.HASH))
        .withAttributeDefinitions(new AttributeDefinition(AccountsDynamoDb.ATTR_ACCOUNT_E164, ScalarAttributeType.S))
        .withProvisionedThroughput(DynamoDbExtension.DEFAULT_PROVISIONED_THROUGHPUT);

    final Table numbersTable = dynamoDbExtension.getDynamoDB().createTable(createNumbersTableRequest);

    final CreateTableRequest createMigrationDeletedAccountsTableRequest = new CreateTableRequest()
        .withTableName(MIGRATION_DELETED_ACCOUNTS_TABLE_NAME)
        .withKeySchema(new KeySchemaElement(MigrationDeletedAccounts.KEY_UUID, KeyType.HASH))
        .withAttributeDefinitions(new AttributeDefinition(MigrationDeletedAccounts.KEY_UUID, ScalarAttributeType.B))
        .withProvisionedThroughput(DynamoDbExtension.DEFAULT_PROVISIONED_THROUGHPUT);

    migrationDeletedAccountsTable = dynamoDbExtension.getDynamoDB().createTable(createMigrationDeletedAccountsTableRequest);

    MigrationDeletedAccounts migrationDeletedAccounts = new MigrationDeletedAccounts(dynamoDbExtension.getDynamoDB(),
        migrationDeletedAccountsTable.getTableName());

    final CreateTableRequest createMigrationRetryAccountsTableRequest = new CreateTableRequest()
        .withTableName(MIGRATION_RETRY_ACCOUNTS_TABLE_NAME)
        .withKeySchema(new KeySchemaElement(MigrationRetryAccounts.KEY_UUID, KeyType.HASH))
        .withAttributeDefinitions(new AttributeDefinition(MigrationRetryAccounts.KEY_UUID, ScalarAttributeType.B))
        .withProvisionedThroughput(DynamoDbExtension.DEFAULT_PROVISIONED_THROUGHPUT);

    migrationRetryAccountsTable = dynamoDbExtension.getDynamoDB().createTable(createMigrationRetryAccountsTableRequest);

    MigrationRetryAccounts migrationRetryAccounts = new MigrationRetryAccounts((dynamoDbExtension.getDynamoDB()),
        migrationRetryAccountsTable.getTableName());

    this.accountsDynamoDb = new AccountsDynamoDb(dynamoDbExtension.getClient(), dynamoDbExtension.getAsyncClient(), new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<>()), dynamoDbExtension.getDynamoDB(), dynamoDbExtension.getTableName(), numbersTable.getTableName(),
        migrationDeletedAccounts, migrationRetryAccounts);
  }

  @Test
  void testStore() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(device));

    accountsDynamoDb.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account);
  }

  @Test
  void testStoreMulti() {
    Set<Device> devices = new HashSet<>();
    devices.add(generateDevice(1));
    devices.add(generateDevice(2));

    Account account = generateAccount("+14151112222", UUID.randomUUID(), devices);

    accountsDynamoDb.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account);
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

    accountsDynamoDb.create(accountFirst);
    accountsDynamoDb.create(accountSecond);

    Optional<Account> retrievedFirst = accountsDynamoDb.get("+14151112222");
    Optional<Account> retrievedSecond = accountsDynamoDb.get("+14152221111");

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, retrievedSecond.get(), accountSecond);

    retrievedFirst = accountsDynamoDb.get(uuidFirst);
    retrievedSecond = accountsDynamoDb.get(uuidSecond);

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

    accountsDynamoDb.create(account);

    verifyStoredState("+14151112222", account.getUuid(), account);

    UUID secondUuid = UUID.randomUUID();

    device = generateDevice(1);
    account = generateAccount("+14151112222", secondUuid, Collections.singleton(device));

    accountsDynamoDb.create(account);
    verifyStoredState("+14151112222", firstUuid, account);

    device = generateDevice(1);
    Account invalidAccount = generateAccount("+14151113333", firstUuid, Collections.singleton(device));

    assertThatThrownBy(() -> accountsDynamoDb.create(invalidAccount));
  }

  @Test
  void testUpdate() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(device));

    accountsDynamoDb.create(account);

    device.setName("foobar");

    accountsDynamoDb.update(account);

    Optional<Account> retrieved = accountsDynamoDb.get("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), retrieved.get(), account);

    retrieved = accountsDynamoDb.get(account.getUuid());

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account);

    device = generateDevice(1);
    Account unknownAccount = generateAccount("+14151113333", UUID.randomUUID(), Collections.singleton(device));

    assertThatThrownBy(() -> accountsDynamoDb.update(unknownAccount)).isInstanceOfAny(ConditionalCheckFailedException.class);

    account.setDynamoDbMigrationVersion(5);

    accountsDynamoDb.update(account);

    verifyStoredState("+14151112222", account.getUuid(), account);
  }

  @Test
  void testDelete() {
    final Device  deletedDevice   = generateDevice (1);
    final Account deletedAccount  = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(deletedDevice));
    final Device  retainedDevice  = generateDevice (1);
    final Account retainedAccount = generateAccount("+14151112345", UUID.randomUUID(), Collections.singleton(retainedDevice));

    accountsDynamoDb.create(deletedAccount);
    accountsDynamoDb.create(retainedAccount);

    assertThat(accountsDynamoDb.get(deletedAccount.getUuid())).isPresent();
    assertThat(accountsDynamoDb.get(retainedAccount.getUuid())).isPresent();

    accountsDynamoDb.delete(deletedAccount.getUuid());

    assertThat(accountsDynamoDb.get(deletedAccount.getUuid())).isNotPresent();

    verifyStoredState(retainedAccount.getNumber(), retainedAccount.getUuid(), accountsDynamoDb.get(retainedAccount.getUuid()).get(), retainedAccount);

    {
      final Account recreatedAccount = generateAccount(deletedAccount.getNumber(), UUID.randomUUID(),
          Collections.singleton(generateDevice(1)));

      accountsDynamoDb.create(recreatedAccount);

      assertThat(accountsDynamoDb.get(recreatedAccount.getUuid())).isPresent();
      verifyStoredState(recreatedAccount.getNumber(), recreatedAccount.getUuid(),
          accountsDynamoDb.get(recreatedAccount.getUuid()).get(), recreatedAccount);
    }

    verifyRecentlyDeletedAccountsTableItemCount(1);

    assertThat(migrationDeletedAccountsTable
        .getItem(MigrationDeletedAccounts.primaryKey(deletedAccount.getUuid()))).isNotNull();

    accountsDynamoDb.deleteRecentlyDeletedUuids();

    verifyRecentlyDeletedAccountsTableItemCount(0);
  }

  private void verifyRecentlyDeletedAccountsTableItemCount(int expectedItemCount) {
    int totalItems = 0;

    for (Page<Item, ScanOutcome> page : migrationDeletedAccountsTable.scan(new ScanSpec()).pages()) {
      for (Item ignored : page) {
        totalItems++;
      }
    }

    assertThat(totalItems).isEqualTo(expectedItemCount);
  }

  @Test
  void testMissing() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", UUID.randomUUID(), Collections.singleton(device));

    accountsDynamoDb.create(account);

    Optional<Account> retrieved = accountsDynamoDb.get("+11111111");
    assertThat(retrieved.isPresent()).isFalse();

    retrieved = accountsDynamoDb.get(UUID.randomUUID());
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

    final AmazonDynamoDB client = mock(AmazonDynamoDB.class);
    final DynamoDB dynamoDB = new DynamoDB(client);

    when(client.transactWriteItems(any()))
        .thenThrow(RuntimeException.class);

    when(client.updateItem(any()))
        .thenThrow(RuntimeException.class);

    AccountsDynamoDb accounts = new AccountsDynamoDb(client, mock(AmazonDynamoDBAsync.class), mock(ThreadPoolExecutor.class), dynamoDB, ACCOUNTS_TABLE_NAME, NUMBERS_TABLE_NAME, mock(
        MigrationDeletedAccounts.class), mock(MigrationRetryAccounts.class));
    Account  account  = generateAccount("+14151112222", UUID.randomUUID());

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
  void testMigrate() throws ExecutionException, InterruptedException {

    Device  device  = generateDevice (1                                            );
    UUID    firstUuid = UUID.randomUUID();
    Account account   = generateAccount("+14151112222", firstUuid, Collections.singleton(device));

    boolean migrated = accountsDynamoDb.migrate(account).get();

    assertThat(migrated).isTrue();

    verifyStoredState("+14151112222", account.getUuid(), account);

    migrated = accountsDynamoDb.migrate(account).get();

    assertThat(migrated).isFalse();

    verifyStoredState("+14151112222", account.getUuid(), account);

    UUID secondUuid = UUID.randomUUID();

    device = generateDevice(1);
    Account accountRemigrationWithDifferentUuid = generateAccount("+14151112222", secondUuid, Collections.singleton(device));

    migrated = accountsDynamoDb.migrate(accountRemigrationWithDifferentUuid).get();

    assertThat(migrated).isFalse();
    verifyStoredState("+14151112222", firstUuid, account);

    account.setDynamoDbMigrationVersion(account.getDynamoDbMigrationVersion() + 1);

    migrated = accountsDynamoDb.migrate(account).get();

    assertThat(migrated).isTrue();
  }

  private Device generateDevice(long id) {
    Random       random       = new Random(System.currentTimeMillis());
    SignedPreKey signedPreKey = new SignedPreKey(random.nextInt(), "testPublicKey-" + random.nextInt(), "testSignature-" + random.nextInt());
    return new Device(id, "testName-" + random.nextInt(), "testAuthToken-" + random.nextInt(), "testSalt-" + random.nextInt(),
        "testGcmId-" + random.nextInt(), "testApnId-" + random.nextInt(), "testVoipApnId-" + random.nextInt(), random.nextBoolean(), random.nextInt(), signedPreKey, random.nextInt(), random.nextInt(), "testUserAgent-" + random.nextInt() , 0, new Device.DeviceCapabilities(random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(),
        false));
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

  private void verifyStoredState(String number, UUID uuid, Account expecting) {
    final Table accounts = dynamoDbExtension.getDynamoDB().getTable(dynamoDbExtension.getTableName());

    Item item = accounts.getItem(new GetItemSpec()
        .withPrimaryKey(AccountsDynamoDb.KEY_ACCOUNT_UUID, UUIDUtil.toByteBuffer(uuid))
        .withConsistentRead(true));

    if (item != null) {
      String data = new String(item.getBinary(AccountsDynamoDb.ATTR_ACCOUNT_DATA), StandardCharsets.UTF_8);
      assertThat(data).isNotEmpty();

      assertThat(item.getNumber(AccountsDynamoDb.ATTR_MIGRATION_VERSION).intValue())
          .isEqualTo(expecting.getDynamoDbMigrationVersion());

      Account result = AccountsDynamoDb.fromItem(item);
      verifyStoredState(number, uuid, result, expecting);
    } else {
      throw new AssertionError("No data");
    }
  }

  private void verifyStoredState(String number, UUID uuid, Account result, Account expecting) {
    assertThat(result.getNumber()).isEqualTo(number);
    assertThat(result.getLastSeen()).isEqualTo(expecting.getLastSeen());
    assertThat(result.getUuid()).isEqualTo(uuid);
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
