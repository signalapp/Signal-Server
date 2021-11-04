/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class AccountsManagerChangeNumberIntegrationTest {

  @RegisterExtension
  static PreparedDbExtension ACCOUNTS_POSTGRES_EXTENSION =
      EmbeddedPostgresExtension.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private static final String ACCOUNTS_TABLE_NAME = "accounts_test";
  private static final String NUMBERS_TABLE_NAME = "numbers_test";
  private static final String NEEDS_RECONCILIATION_INDEX_NAME = "needs_reconciliation_test";
  private static final String DELETED_ACCOUNTS_LOCK_TABLE_NAME = "deleted_accounts_lock_test";
  private static final int SCAN_PAGE_SIZE = 1;

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
  static DynamoDbExtension DELETED_ACCOUNTS_DYNAMO_EXTENSION = DynamoDbExtension.builder()
      .tableName("deleted_accounts_test")
      .hashKey(DeletedAccounts.KEY_ACCOUNT_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.KEY_ACCOUNT_E164)
          .attributeType(ScalarAttributeType.S).build())
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.ATTR_NEEDS_CDS_RECONCILIATION)
          .attributeType(ScalarAttributeType.N)
          .build())
      .globalSecondaryIndex(GlobalSecondaryIndex.builder()
          .indexName(NEEDS_RECONCILIATION_INDEX_NAME)
          .keySchema(KeySchemaElement.builder().attributeName(DeletedAccounts.KEY_ACCOUNT_E164).keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName(DeletedAccounts.ATTR_NEEDS_CDS_RECONCILIATION).keyType(KeyType.RANGE).build())
          .projection(Projection.builder().projectionType(ProjectionType.INCLUDE).nonKeyAttributes(DeletedAccounts.ATTR_ACCOUNT_UUID).build())
          .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
          .build())
      .build();

  @RegisterExtension
  static DynamoDbExtension DELETED_ACCOUNTS_LOCK_DYNAMO_EXTENSION = DynamoDbExtension.builder()
      .tableName(DELETED_ACCOUNTS_LOCK_TABLE_NAME)
      .hashKey(DeletedAccounts.KEY_ACCOUNT_E164)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(DeletedAccounts.KEY_ACCOUNT_E164)
          .attributeType(ScalarAttributeType.S).build())
      .build();

  @RegisterExtension
  static RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private ClientPresenceManager clientPresenceManager;
  private DeletedAccounts deletedAccounts;

  private AccountsManager accountsManager;

  @BeforeEach
  void setup() throws InterruptedException {

    {
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
    }

    final Accounts accounts = new Accounts(
        ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient(),
        ACCOUNTS_DYNAMO_EXTENSION.getTableName(),
        NUMBERS_TABLE_NAME,
        SCAN_PAGE_SIZE);

    {
      final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

      DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
      when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

      deletedAccounts = new DeletedAccounts(DELETED_ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient(),
          DELETED_ACCOUNTS_DYNAMO_EXTENSION.getTableName(),
          NEEDS_RECONCILIATION_INDEX_NAME);

      final DeletedAccountsManager deletedAccountsManager = new DeletedAccountsManager(deletedAccounts,
          DELETED_ACCOUNTS_LOCK_DYNAMO_EXTENSION.getLegacyDynamoClient(),
          DELETED_ACCOUNTS_LOCK_DYNAMO_EXTENSION.getTableName());

      final SecureStorageClient secureStorageClient = mock(SecureStorageClient.class);
      when(secureStorageClient.deleteStoredData(any())).thenReturn(CompletableFuture.completedFuture(null));

      final SecureBackupClient secureBackupClient = mock(SecureBackupClient.class);
      when(secureBackupClient.deleteBackups(any())).thenReturn(CompletableFuture.completedFuture(null));

      clientPresenceManager = mock(ClientPresenceManager.class);

      accountsManager = new AccountsManager(
          accounts,
          CACHE_CLUSTER_EXTENSION.getRedisCluster(),
          deletedAccountsManager,
          mock(DirectoryQueue.class),
          mock(KeysDynamoDb.class),
          mock(MessagesManager.class),
          mock(UsernamesManager.class),
          mock(ProfilesManager.class),
          mock(StoredVerificationCodeManager.class),
          secureStorageClient,
          secureBackupClient,
          clientPresenceManager,
          mock(Clock.class));
    }
  }

  @Test
  void testChangeNumber() throws InterruptedException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();

    accountsManager.changeNumber(account, secondNumber);

    assertTrue(accountsManager.get(originalNumber).isEmpty());

    assertTrue(accountsManager.get(secondNumber).isPresent());
    assertEquals(Optional.of(originalUuid), accountsManager.get(secondNumber).map(Account::getUuid));

    assertEquals(secondNumber, accountsManager.get(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
  }

  @Test
  void testChangeNumberReturnToOriginal() throws InterruptedException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();

    account = accountsManager.changeNumber(account, secondNumber);
    accountsManager.changeNumber(account, originalNumber);

    assertTrue(accountsManager.get(originalNumber).isPresent());
    assertEquals(Optional.of(originalUuid), accountsManager.get(originalNumber).map(Account::getUuid));

    assertTrue(accountsManager.get(secondNumber).isEmpty());

    assertEquals(originalNumber, accountsManager.get(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
  }

  @Test
  void testChangeNumberContested() throws InterruptedException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();

    final Account existingAccount = accountsManager.create(secondNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID existingAccountUuid = existingAccount.getUuid();

    accountsManager.update(existingAccount, a -> a.addDevice(new Device(Device.MASTER_ID, "test", "token", "salt", null, null, null, true, 1, null, 0, 0, null, 0, new DeviceCapabilities())));

    accountsManager.changeNumber(account, secondNumber);

    assertTrue(accountsManager.get(originalNumber).isEmpty());

    assertTrue(accountsManager.get(secondNumber).isPresent());
    assertEquals(Optional.of(originalUuid), accountsManager.get(secondNumber).map(Account::getUuid));

    assertEquals(secondNumber, accountsManager.get(originalUuid).map(Account::getNumber).orElseThrow());

    verify(clientPresenceManager).displacePresence(existingAccountUuid, Device.MASTER_ID);

    assertEquals(Optional.of(existingAccountUuid), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
  }

  @Test
  void testChangeNumberChaining() throws InterruptedException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();

    final Account existingAccount = accountsManager.create(secondNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID existingAccountUuid = existingAccount.getUuid();

    accountsManager.changeNumber(account, secondNumber);

    final Account reRegisteredAccount = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());

    assertEquals(existingAccountUuid, reRegisteredAccount.getUuid());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));

    accountsManager.changeNumber(reRegisteredAccount, secondNumber);

    assertEquals(Optional.of(originalUuid), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
  }
}
