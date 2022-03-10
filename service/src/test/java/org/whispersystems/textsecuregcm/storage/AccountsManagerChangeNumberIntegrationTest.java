/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

  private static final String ACCOUNTS_TABLE_NAME = "accounts_test";
  private static final String NUMBERS_TABLE_NAME = "numbers_test";
  private static final String PNI_ASSIGNMENT_TABLE_NAME = "pni_assignment_test";
  private static final String USERNAMES_TABLE_NAME = "usernames_test";
  private static final String PNI_TABLE_NAME = "pni_test";
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

    {
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
    }

    {
      @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
          mock(DynamicConfigurationManager.class);

      DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
      when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

      final Accounts accounts = new Accounts(
          dynamicConfigurationManager,
          ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient(),
          ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbAsyncClient(),
          ACCOUNTS_DYNAMO_EXTENSION.getTableName(),
          NUMBERS_TABLE_NAME,
          PNI_ASSIGNMENT_TABLE_NAME,
          USERNAMES_TABLE_NAME,
          SCAN_PAGE_SIZE);

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

      final PhoneNumberIdentifiers phoneNumberIdentifiers =
          new PhoneNumberIdentifiers(PNI_DYNAMO_EXTENSION.getDynamoDbClient(), PNI_TABLE_NAME);

      accountsManager = new AccountsManager(
          accounts,
          phoneNumberIdentifiers,
          CACHE_CLUSTER_EXTENSION.getRedisCluster(),
          deletedAccountsManager,
          mock(DirectoryQueue.class),
          mock(Keys.class),
          mock(MessagesManager.class),
          mock(ReservedUsernames.class),
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
    final UUID originalPni = account.getPhoneNumberIdentifier();

    accountsManager.changeNumber(account, secondNumber);

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    assertTrue(accountsManager.getByE164(secondNumber).isPresent());
    assertEquals(originalUuid, accountsManager.getByE164(secondNumber).map(Account::getUuid).orElseThrow());
    assertNotEquals(originalPni, accountsManager.getByE164(secondNumber).map(Account::getPhoneNumberIdentifier).orElseThrow());

    assertEquals(secondNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
  }

  @Test
  void testChangeNumberReturnToOriginal() throws InterruptedException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    account = accountsManager.changeNumber(account, secondNumber);
    accountsManager.changeNumber(account, originalNumber);

    assertTrue(accountsManager.getByE164(originalNumber).isPresent());
    assertEquals(originalUuid, accountsManager.getByE164(originalNumber).map(Account::getUuid).orElseThrow());
    assertEquals(originalPni, accountsManager.getByE164(originalNumber).map(Account::getPhoneNumberIdentifier).orElseThrow());

    assertTrue(accountsManager.getByE164(secondNumber).isEmpty());

    assertEquals(originalNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

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

    assertTrue(accountsManager.getByE164(originalNumber).isEmpty());

    assertTrue(accountsManager.getByE164(secondNumber).isPresent());
    assertEquals(Optional.of(originalUuid), accountsManager.getByE164(secondNumber).map(Account::getUuid));

    assertEquals(secondNumber, accountsManager.getByAccountIdentifier(originalUuid).map(Account::getNumber).orElseThrow());

    verify(clientPresenceManager).disconnectPresence(existingAccountUuid, Device.MASTER_ID);

    assertEquals(Optional.of(existingAccountUuid), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));

    accountsManager.changeNumber(accountsManager.getByAccountIdentifier(originalUuid).orElseThrow(), originalNumber);

    final Account existingAccount2 = accountsManager.create(secondNumber, "password", null, new AccountAttributes(),
        new ArrayList<>());

    assertEquals(existingAccountUuid, existingAccount2.getUuid());
  }

  @Test
  void testChangeNumberChaining() throws InterruptedException {
    final String originalNumber = "+18005551111";
    final String secondNumber = "+18005552222";

    final Account account = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID originalUuid = account.getUuid();
    final UUID originalPni = account.getPhoneNumberIdentifier();

    final Account existingAccount = accountsManager.create(secondNumber, "password", null, new AccountAttributes(), new ArrayList<>());
    final UUID existingAccountUuid = existingAccount.getUuid();

    final Account changedNumberAccount = accountsManager.changeNumber(account, secondNumber);
    final UUID secondPni = changedNumberAccount.getPhoneNumberIdentifier();

    final Account reRegisteredAccount = accountsManager.create(originalNumber, "password", null, new AccountAttributes(), new ArrayList<>());

    assertEquals(existingAccountUuid, reRegisteredAccount.getUuid());
    assertEquals(originalPni, reRegisteredAccount.getPhoneNumberIdentifier());

    assertEquals(Optional.empty(), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));

    final Account changedNumberReRegisteredAccount = accountsManager.changeNumber(reRegisteredAccount, secondNumber);

    assertEquals(Optional.of(originalUuid), deletedAccounts.findUuid(originalNumber));
    assertEquals(Optional.empty(), deletedAccounts.findUuid(secondNumber));
    assertEquals(secondPni, changedNumberReRegisteredAccount.getPhoneNumberIdentifier());
  }
}
