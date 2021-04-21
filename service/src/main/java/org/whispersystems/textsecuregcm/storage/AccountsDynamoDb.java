package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class AccountsDynamoDb extends AbstractDynamoDbStore implements AccountStore {

  // uuid, primary key
  static final String KEY_ACCOUNT_UUID = "U";
  // phone number
  static final String ATTR_ACCOUNT_E164 = "P";
  // account, serialized to JSON
  static final String ATTR_ACCOUNT_DATA = "D";

  static final String ATTR_MIGRATION_VERSION = "V";

  private final AmazonDynamoDB client;
  private final Table accountsTable;
  private final AmazonDynamoDBAsync asyncClient;

  private final ThreadPoolExecutor migrationThreadPool;

  private final MigrationDeletedAccounts migrationDeletedAccounts;
  private final MigrationRetryAccounts migrationRetryAccounts;

  private final String phoneNumbersTableName;

  private static final Timer CREATE_TIMER = Metrics.timer(name(AccountsDynamoDb.class, "create"));
  private static final Timer UPDATE_TIMER = Metrics.timer(name(AccountsDynamoDb.class, "update"));
  private static final Timer GET_BY_NUMBER_TIMER = Metrics.timer(name(AccountsDynamoDb.class, "getByNumber"));
  private static final Timer GET_BY_UUID_TIMER = Metrics.timer(name(AccountsDynamoDb.class, "getByUuid"));
  private static final Timer DELETE_TIMER = Metrics.timer(name(AccountsDynamoDb.class, "delete"));

  private final Logger logger = LoggerFactory.getLogger(AccountsDynamoDb.class);

  public AccountsDynamoDb(AmazonDynamoDB client, AmazonDynamoDBAsync asyncClient,
      ThreadPoolExecutor migrationThreadPool, DynamoDB dynamoDb, String accountsTableName, String phoneNumbersTableName,
      MigrationDeletedAccounts migrationDeletedAccounts,
      MigrationRetryAccounts accountsMigrationErrors) {

    super(dynamoDb);

    this.client = client;
    this.accountsTable = dynamoDb.getTable(accountsTableName);
    this.phoneNumbersTableName = phoneNumbersTableName;

    this.asyncClient = asyncClient;
    this.migrationThreadPool = migrationThreadPool;

    this.migrationDeletedAccounts = migrationDeletedAccounts;
    this.migrationRetryAccounts = accountsMigrationErrors;
  }

  @Override
  public boolean create(Account account) {

    return CREATE_TIMER.record(() -> {

      try {
        TransactWriteItem phoneNumberConstraintPut = buildPutWriteItemForPhoneNumberConstraint(account, account.getUuid());

        TransactWriteItem accountPut = buildPutWriteItemForAccount(account, account.getUuid());

        final TransactWriteItemsRequest request = new TransactWriteItemsRequest()
            .withTransactItems(phoneNumberConstraintPut, accountPut);

        try {
          client.transactWriteItems(request);
        } catch (TransactionCanceledException e) {

          final CancellationReason accountCancellationReason = e.getCancellationReasons().get(1);

          if ("ConditionalCheckFailed".equals(accountCancellationReason.getCode())) {
            throw new IllegalArgumentException("uuid present with different phone number");
          }

          final CancellationReason phoneNumberConstraintCancellationReason = e.getCancellationReasons().get(0);

          if ("ConditionalCheckFailed".equals(phoneNumberConstraintCancellationReason.getCode())) {

            ByteBuffer actualAccountUuid = phoneNumberConstraintCancellationReason.getItem().get(KEY_ACCOUNT_UUID).getB();
            account.setUuid(UUIDUtil.fromByteBuffer(actualAccountUuid));

            update(account);

            return false;
          }

          // this shouldn’t happen
          throw new RuntimeException("could not create account");
        }
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      return true;
    });
  }

  private TransactWriteItem buildPutWriteItemForAccount(Account account, UUID uuid) throws JsonProcessingException {
    return new TransactWriteItem()
        .withPut(
            new Put()
                .withTableName(accountsTable.getTableName())
                .withItem(Map.of(
                    KEY_ACCOUNT_UUID, new AttributeValue().withB(UUIDUtil.toByteBuffer(uuid)),
                    ATTR_ACCOUNT_E164, new AttributeValue(account.getNumber()),
                    ATTR_ACCOUNT_DATA, new AttributeValue()
                        .withB(ByteBuffer.wrap(SystemMapper.getMapper().writeValueAsBytes(account))),
                    ATTR_MIGRATION_VERSION, new AttributeValue().withN(
                        String.valueOf(account.getDynamoDbMigrationVersion()))))
                .withConditionExpression("attribute_not_exists(#number) OR #number = :number")
                .withExpressionAttributeNames(Map.of("#number", ATTR_ACCOUNT_E164))
                .withExpressionAttributeValues(Map.of(":number", new AttributeValue(account.getNumber()))));
  }

  private TransactWriteItem buildPutWriteItemForPhoneNumberConstraint(Account account, UUID uuid) {
    return new TransactWriteItem()
        .withPut(
            new Put()
                .withTableName(phoneNumbersTableName)
                .withItem(Map.of(
                    ATTR_ACCOUNT_E164, new AttributeValue(account.getNumber()),
                    KEY_ACCOUNT_UUID, new AttributeValue().withB(UUIDUtil.toByteBuffer(uuid))))
                .withConditionExpression(
                    "attribute_not_exists(#number) OR (attribute_exists(#number) AND #uuid = :uuid)")
                .withExpressionAttributeNames(
                    Map.of("#uuid", KEY_ACCOUNT_UUID,
                        "#number", ATTR_ACCOUNT_E164))
                .withExpressionAttributeValues(
                    Map.of(":uuid", new AttributeValue().withB(UUIDUtil.toByteBuffer(uuid))))
                .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD));
  }

  @Override
  public void update(Account account) {
    UPDATE_TIMER.record(() -> {
      UpdateItemRequest updateItemRequest;
      try {
        updateItemRequest = new UpdateItemRequest()
            .withTableName(accountsTable.getTableName())
            .withKey(Map.of(KEY_ACCOUNT_UUID, new AttributeValue().withB(UUIDUtil.toByteBuffer(account.getUuid()))))
            .withUpdateExpression("SET #data = :data, #version = :version")
            .withConditionExpression("attribute_exists(#number)")
            .withExpressionAttributeNames(Map.of("#number", ATTR_ACCOUNT_E164,
                "#data", ATTR_ACCOUNT_DATA,
                "#version", ATTR_MIGRATION_VERSION))
            .withExpressionAttributeValues(Map.of(":data", new AttributeValue().withB(ByteBuffer.wrap(SystemMapper.getMapper().writeValueAsBytes(account))),
                ":version", new AttributeValue().withN(String.valueOf(account.getDynamoDbMigrationVersion()))));

      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      client.updateItem(updateItemRequest);
    });
  }

  @Override
  public Optional<Account> get(String number) {

    return GET_BY_NUMBER_TIMER.record(() -> {

      final GetItemResult phoneNumberAndUuid = client.getItem(phoneNumbersTableName,
          Map.of(ATTR_ACCOUNT_E164, new AttributeValue(number)), true);

      return Optional.ofNullable(phoneNumberAndUuid.getItem())
          .map(item -> item.get(KEY_ACCOUNT_UUID).getB())
          .map(uuid -> accountsTable.getItem(new GetItemSpec()
              .withPrimaryKey(KEY_ACCOUNT_UUID, uuid.array())
              .withConsistentRead(true)))
          .map(AccountsDynamoDb::fromItem);
    });
  }

  @Override
  public Optional<Account> get(UUID uuid) {
    Optional<Item> maybeItem = GET_BY_UUID_TIMER.record(() ->
        Optional.ofNullable(accountsTable.getItem(new GetItemSpec().
            withPrimaryKey(new PrimaryKey(KEY_ACCOUNT_UUID, UUIDUtil.toByteBuffer(uuid)))
            .withConsistentRead(true))));

    return maybeItem.map(AccountsDynamoDb::fromItem);
  }

  @Override
  public void delete(UUID uuid) {
    DELETE_TIMER.record(() -> {

      delete(uuid, true);
    });
  }

  private void delete(UUID uuid, boolean saveInDeletedAccountsTable) {

    if (saveInDeletedAccountsTable) {
      migrationDeletedAccounts.put(uuid);
    }

    Optional<Account> maybeAccount = get(uuid);

    maybeAccount.ifPresent(account -> {

      TransactWriteItem phoneNumberDelete = new TransactWriteItem()
          .withDelete(new Delete()
              .withTableName(phoneNumbersTableName)
              .withKey(Map.of(ATTR_ACCOUNT_E164, new AttributeValue(account.getNumber()))));

      TransactWriteItem accountDelete = new TransactWriteItem().withDelete(
          new Delete()
              .withTableName(accountsTable.getTableName())
              .withKey(Map.of(KEY_ACCOUNT_UUID, new AttributeValue().withB(UUIDUtil.toByteBuffer(uuid)))));

      TransactWriteItemsRequest request = new TransactWriteItemsRequest()
          .withTransactItems(phoneNumberDelete, accountDelete);

      client.transactWriteItems(request);
    });
  }

  private static final Counter MIGRATED_COUNTER = Metrics.counter(name(AccountsDynamoDb.class, "migration", "count"));
  private static final Counter ERROR_COUNTER = Metrics.counter(name(AccountsDynamoDb.class, "migration", "error"));

  public CompletableFuture<Void> migrate(List<Account> accounts, int threads) {

    migrationThreadPool.setCorePoolSize(threads);
    migrationThreadPool.setMaximumPoolSize(threads);

    final List<CompletableFuture<?>> futures = accounts.stream()
        .map(this::migrate)
        .map(f -> f.whenComplete((migrated, e) -> {
          if (e == null) {
            MIGRATED_COUNTER.increment(migrated ? 1 : 0);
          } else {
            ERROR_COUNTER.increment();
          }
        }))
        .collect(Collectors.toList());

    CompletableFuture<Void> migrationBatch = CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{}));

    return migrationBatch.whenComplete((result, exception) -> deleteRecentlyDeletedUuids());
  }

  public void deleteRecentlyDeletedUuids() {

    final List<UUID> recentlyDeletedUuids = migrationDeletedAccounts.getRecentlyDeletedUuids();

    for (UUID recentlyDeletedUuid : recentlyDeletedUuids) {
      delete(recentlyDeletedUuid, false);
    }

    migrationDeletedAccounts.delete(recentlyDeletedUuids);
  }

  public CompletableFuture<Boolean> migrate(Account account) {
    try {
      TransactWriteItem phoneNumberConstraintPut = buildPutWriteItemForPhoneNumberConstraint(account, account.getUuid());

      TransactWriteItem accountPut = buildPutWriteItemForAccount(account, account.getUuid());
      accountPut.getPut()
          .setConditionExpression("attribute_not_exists(#uuid) OR (attribute_exists(#uuid) AND #version < :version)");
      accountPut.getPut()
          .setExpressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID,
              "#version", ATTR_MIGRATION_VERSION));
      accountPut.getPut()
          .setExpressionAttributeValues(
              Map.of(":version", new AttributeValue().withN(String.valueOf(account.getDynamoDbMigrationVersion()))));

      final TransactWriteItemsRequest request = new TransactWriteItemsRequest()
          .withTransactItems(phoneNumberConstraintPut, accountPut);

      final CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();

      asyncClient.transactWriteItemsAsync(request,
          new AsyncHandler<>() {
            @Override
            public void onError(Exception exception) {
              if (exception instanceof TransactionCanceledException) {
                // account is already migrated
                resultFuture.complete(false);
              } else {
                try {
                  migrationRetryAccounts.put(account.getUuid());
                } catch (final Exception e) {
                  logger.error("Could not store account {}", account.getUuid());
                }
                resultFuture.completeExceptionally(exception);
              }
            }

            @Override
            public void onSuccess(TransactWriteItemsRequest request, TransactWriteItemsResult transactWriteItemsResult) {
              resultFuture.complete(true);
            }
          });

      return resultFuture;

    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  @VisibleForTesting
  static Account fromItem(Item item) {
    try {
      Account account = SystemMapper.getMapper().readValue(item.getBinary(ATTR_ACCOUNT_DATA), Account.class);

      account.setNumber(item.getString(ATTR_ACCOUNT_E164));
      account.setUuid(UUIDUtil.fromByteBuffer(item.getByteBuffer(KEY_ACCOUNT_UUID)));

      return account;

    } catch (IOException e) {
      throw new RuntimeException("Could not read stored account data", e);
    }
  }
}
