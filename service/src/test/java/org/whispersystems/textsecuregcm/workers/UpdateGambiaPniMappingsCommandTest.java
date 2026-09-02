/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.AccountLockManager;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberWaitingPeriodManager;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.PhoneNumberRecoveryPasswords;
import org.whispersystems.textsecuregcm.storage.PhoneNumberRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.storage.S3LocalStackExtension;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

class UpdateGambiaPniMappingsCommandTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.ACCOUNTS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS,
      DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK,
      DynamoDbExtensionSchema.Tables.NUMBERS,
      DynamoDbExtensionSchema.Tables.PNI,
      DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS,
      DynamoDbExtensionSchema.Tables.USERNAMES,
      DynamoDbExtensionSchema.Tables.PHONE_NUMBER_RECOVERY_PASSWORDS,
      DynamoDbExtensionSchema.Tables.REDEEMED_RECEIPTS);

  @RegisterExtension
  static final RedisClusterExtension CACHE_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension("testbucket");

  private ScheduledExecutorService executor;
  private AccountsManager accountsManager;

  private TestUpdateGambiaPniMappingsCommand updateGambiaPniMappingsCommand;

  private static class TestUpdateGambiaPniMappingsCommand extends UpdateGambiaPniMappingsCommand {

    private final PhoneNumberIdentifiers phoneNumberIdentifiers;

    private TestUpdateGambiaPniMappingsCommand(final PhoneNumberIdentifiers phoneNumberIdentifiers) {
      this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    }

    @Override
    protected CommandDependencies getCommandDependencies() {
      return new CommandDependencies(null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          phoneNumberIdentifiers,
          null,
          null);
    }
  }

  @BeforeEach
  void setup() {
    final Accounts accounts = new Accounts(
        Clock.systemUTC(),
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        new RedeemedReceiptsManager(Clock.systemUTC(), DynamoDbExtensionSchema.Tables.REDEEMED_RECEIPTS.tableName(),
            DYNAMO_DB_EXTENSION.getDynamoDbClient()),
        DynamoDbExtensionSchema.Tables.ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.NUMBERS.tableName(),
        DynamoDbExtensionSchema.Tables.PNI_ASSIGNMENTS.tableName(),
        DynamoDbExtensionSchema.Tables.USERNAMES.tableName(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS.tableName(),
        DynamoDbExtensionSchema.Tables.USED_LINK_DEVICE_TOKENS.tableName());

    executor = Executors.newSingleThreadScheduledExecutor();

    final AccountLockManager accountLockManager = new AccountLockManager(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DynamoDbExtensionSchema.Tables.DELETED_ACCOUNTS_LOCK.tableName());

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
            DynamoDbExtensionSchema.Tables.PNI.tableName());

    final PhoneNumberRecoveryPasswords phoneNumberRecoveryPasswords =
        new PhoneNumberRecoveryPasswords(DynamoDbExtensionSchema.Tables.PHONE_NUMBER_RECOVERY_PASSWORDS.tableName(),
            Duration.ofDays(1),
            DYNAMO_DB_EXTENSION.getDynamoDbClient(),
            Clock.systemUTC());

    final PhoneNumberRecoveryPasswordsManager phoneNumberRecoveryPasswordsManager =
        new PhoneNumberRecoveryPasswordsManager(phoneNumberRecoveryPasswords);

    accountsManager = new AccountsManager(
        accounts,
        phoneNumberIdentifiers,
        CACHE_CLUSTER_EXTENSION.getRedisCluster(),
        mock(FaultTolerantRedisClient.class),
        accountLockManager,
        mock(KeysManager.class),
        mock(MessagesManager.class),
        mock(ProfilesManager.class),
        mock(ChangeNumberWaitingPeriodManager.class),
        mock(SecureStorageClient.class),
        mock(SecureValueRecoveryClient.class),
        mock(DisconnectionRequestManager.class),
        phoneNumberRecoveryPasswordsManager,
        executor,
        executor,
        executor,
        mock(Clock.class),
        "link-device-secret".getBytes(StandardCharsets.UTF_8),
        AccountsManager.TOTP.getTimeStep().dividedBy(2));

    updateGambiaPniMappingsCommand = new TestUpdateGambiaPniMappingsCommand(phoneNumberIdentifiers);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdown();

    //noinspection ResultOfMethodCallIgnored
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void crawlAccounts() {
    final String legacyGambianNumber = "+2203123456";
    final Set<String> alternateForms = new HashSet<>(Util.getAlternateForms(legacyGambianNumber));
    alternateForms.remove(legacyGambianNumber);

    assertFalse(alternateForms.isEmpty());

    AccountsHelper.createAccount(accountsManager, legacyGambianNumber);

    // To simulate existing numbers with only a single form mapped to a PNI, artificially remove alternate PNI mappings
    alternateForms.forEach(e164 -> DYNAMO_DB_EXTENSION.getDynamoDbClient().deleteItem(DeleteItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.PNI.tableName())
            .key(Map.of(PhoneNumberIdentifiers.KEY_E164, AttributeValues.fromString(e164)))
        .build()));

    alternateForms.forEach(e164 -> assertFalse(DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.PNI.tableName())
            .key(Map.of(PhoneNumberIdentifiers.KEY_E164, AttributeValues.fromString(e164)))
            .build())
        .hasItem()));

    updateGambiaPniMappingsCommand.crawlAccounts(accountsManager.streamAllFromDynamo(1, Schedulers.boundedElastic()));

    alternateForms.forEach(e164 -> assertTrue(DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.PNI.tableName())
            .key(Map.of(PhoneNumberIdentifiers.KEY_E164, AttributeValues.fromString(e164)))
            .build())
        .hasItem()));

    assertTrue(accountsManager.getByE164(legacyGambianNumber).isPresent());
  }
}
