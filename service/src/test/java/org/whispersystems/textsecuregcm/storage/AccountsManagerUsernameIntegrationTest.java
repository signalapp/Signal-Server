/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

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
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.UsernameGenerator;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.time.Clock;
import java.util.*;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AccountsManagerUsernameIntegrationTest {

  private static final String ACCOUNTS_TABLE_NAME = "accounts_test";
  private static final String NUMBERS_TABLE_NAME = "numbers_test";
  private static final String PNI_ASSIGNMENT_TABLE_NAME = "pni_assignment_test";
  private static final String USERNAMES_TABLE_NAME = "usernames_test";
  private static final String PNI_TABLE_NAME = "pni_test";
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
            .attributeName(Accounts.ATTR_USERNAME)
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(Accounts.ATTR_USERNAME)
            .attributeType(ScalarAttributeType.S)
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

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    accounts = Mockito.spy(new Accounts(
        dynamicConfigurationManager,
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
        mock(ReservedUsernames.class),
        mock(ProfilesManager.class),
        mock(StoredVerificationCodeManager.class),
        mock(SecureStorageClient.class),
        mock(SecureBackupClient.class),
        mock(ClientPresenceManager.class),
        new UsernameGenerator(1, 2, 10),
        experimentEnrollmentManager,
        mock(Clock.class));
  }

  private static int discriminator(String username) {
    return Integer.parseInt(username.split(UsernameGenerator.SEPARATOR)[1]);
  }

  @Test
  void testSetClearUsername() throws UsernameNotAvailableException, InterruptedException {
    Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());
    account = accountsManager.setUsername(account, "n00bkiller", null);
    assertThat(account.getUsername()).isPresent();
    assertThat(account.getUsername().get()).startsWith("n00bkiller");
    int discriminator = discriminator(account.getUsername().get());
    assertThat(discriminator).isGreaterThan(0).isLessThan(10);

    assertThat(accountsManager.getByUsername(account.getUsername().get()).orElseThrow().getUuid()).isEqualTo(
        account.getUuid());

    // reroll
    account = accountsManager.setUsername(account, "n00bkiller", account.getUsername().get());
    final String newUsername = account.getUsername().orElseThrow();
    assertThat(discriminator(account.getUsername().orElseThrow())).isNotEqualTo(discriminator);

    // clear
    account = accountsManager.clearUsername(account);
    assertThat(accountsManager.getByUsername(newUsername)).isEmpty();
    assertThat(accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getUsername()).isEmpty();
  }

  @Test
  void testNoUsernames() throws InterruptedException {
    Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());
    for (int i = 1; i <= 99; i++) {
      ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
          .tableName(USERNAMES_TABLE_NAME)
          .item(Map.of(
              Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(UUID.randomUUID()),
              Accounts.ATTR_USERNAME, AttributeValues.fromString(UsernameGenerator.fromParts("n00bkiller", i))))
          .build());
    }
    assertThrows(UsernameNotAvailableException.class, () -> accountsManager.setUsername(account, "n00bkiller", null));
    assertThat(accountsManager.getByAccountIdentifier(account.getUuid()).orElseThrow().getUsername()).isEmpty();
  }

  @Test
  void testUsernameSnatched() throws InterruptedException, UsernameNotAvailableException {
    final Account account = accountsManager.create("+18005551111", "password", null, new AccountAttributes(),
        new ArrayList<>());
    for (int i = 1; i <= 9; i++) {
      ACCOUNTS_DYNAMO_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
          .tableName(USERNAMES_TABLE_NAME)
          .item(Map.of(
              Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(UUID.randomUUID()),
              Accounts.ATTR_USERNAME, AttributeValues.fromString(UsernameGenerator.fromParts("n00bkiller", i))))
          .build());
    }

    // first time this is called lie and say the username is available
    // this simulates seeing an available username and then it being taken
    // by someone before the write
    doReturn(true).doCallRealMethod().when(accounts).usernameAvailable(any());
    final String username = accountsManager
        .setUsername(account, "n00bkiller", null)
        .getUsername().orElseThrow();
    assertThat(username).startsWith("n00bkiller");
    assertThat(discriminator(username)).isGreaterThanOrEqualTo(10).isLessThan(100);

    // 1 attempt on first try (returns true),
    // 10 (attempts per width) on width=2 discriminators (all taken)
    verify(accounts, times(11)).usernameAvailable(argThat(un -> discriminator(un) < 10));

    // 1 final attempt on width=3 discriminators
    verify(accounts, times(1)).usernameAvailable(argThat(un -> discriminator(un) >= 10));
  }

}
