/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class Accounts extends AbstractDynamoDbStore {

  // uuid, primary key
  static final String KEY_ACCOUNT_UUID = "U";
  // phone number
  static final String ATTR_ACCOUNT_E164 = "P";
  // account, serialized to JSON
  static final String ATTR_ACCOUNT_DATA = "D";
  // internal version for optimistic locking
  static final String ATTR_VERSION = "V";
  // canonically discoverable
  static final String ATTR_CANONICALLY_DISCOVERABLE = "C";

  private final DynamoDbClient client;

  private final String phoneNumbersTableName;
  private final String accountsTableName;

  private final int scanPageSize;

  private static final Timer CREATE_TIMER = Metrics.timer(name(Accounts.class, "create"));
  private static final Timer UPDATE_TIMER = Metrics.timer(name(Accounts.class, "update"));
  private static final Timer GET_BY_NUMBER_TIMER = Metrics.timer(name(Accounts.class, "getByNumber"));
  private static final Timer GET_BY_UUID_TIMER = Metrics.timer(name(Accounts.class, "getByUuid"));
  private static final Timer GET_ALL_FROM_START_TIMER = Metrics.timer(name(Accounts.class, "getAllFrom"));
  private static final Timer GET_ALL_FROM_OFFSET_TIMER = Metrics.timer(name(Accounts.class, "getAllFromOffset"));
  private static final Timer DELETE_TIMER = Metrics.timer(name(Accounts.class, "delete"));


  public Accounts(DynamoDbClient client, String accountsTableName, String phoneNumbersTableName,
      final int scanPageSize) {

    super(client);

    this.client = client;
    this.phoneNumbersTableName = phoneNumbersTableName;
    this.accountsTableName = accountsTableName;
    this.scanPageSize = scanPageSize;
  }

  public boolean create(Account account) {
    return CREATE_TIMER.record(() -> {

      try {
        TransactWriteItem phoneNumberConstraintPut = buildPutWriteItemForPhoneNumberConstraint(account, account.getUuid());
        TransactWriteItem accountPut = buildPutWriteItemForAccount(account, account.getUuid(), Put.builder()
            .conditionExpression("attribute_not_exists(#number) OR #number = :number")
            .expressionAttributeNames(Map.of("#number", ATTR_ACCOUNT_E164))
            .expressionAttributeValues(Map.of(":number", AttributeValues.fromString(account.getNumber()))));

        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(phoneNumberConstraintPut, accountPut)
            .build();

        try {
          client.transactWriteItems(request);
        } catch (TransactionCanceledException e) {

          final CancellationReason accountCancellationReason = e.cancellationReasons().get(1);

          if ("ConditionalCheckFailed".equals(accountCancellationReason.code())) {
            throw new IllegalArgumentException("uuid present with different phone number");
          }

          final CancellationReason phoneNumberConstraintCancellationReason = e.cancellationReasons().get(0);

          if ("ConditionalCheckFailed".equals(phoneNumberConstraintCancellationReason.code())) {

            ByteBuffer actualAccountUuid = phoneNumberConstraintCancellationReason.item().get(KEY_ACCOUNT_UUID).b().asByteBuffer();
            account.setUuid(UUIDUtil.fromByteBuffer(actualAccountUuid));

            final int version = get(account.getUuid()).get().getVersion();
            account.setVersion(version);

            update(account);

            return false;
          }

          if ("TransactionConflict".equals(accountCancellationReason.code())) {
            // this should only happen if two clients manage to make concurrent create() calls
            throw new ContestedOptimisticLockException();
          }

          // this shouldn’t happen
          throw new RuntimeException("could not create account: " + extractCancellationReasonCodes(e));
        }
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      return true;
    });
  }

  private TransactWriteItem buildPutWriteItemForAccount(Account account, UUID uuid, Put.Builder putBuilder) throws JsonProcessingException {
    return TransactWriteItem.builder()
        .put(putBuilder
            .tableName(accountsTableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber()),
                ATTR_ACCOUNT_DATA, AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
                ATTR_VERSION, AttributeValues.fromInt(account.getVersion()),
                ATTR_CANONICALLY_DISCOVERABLE, AttributeValues.fromBool(account.shouldBeVisibleInDirectory())))
            .build())
        .build();
  }

  private TransactWriteItem buildPutWriteItemForPhoneNumberConstraint(Account account, UUID uuid) {
    return TransactWriteItem.builder()
        .put(
            Put.builder()
                .tableName(phoneNumbersTableName)
                .item(Map.of(
                    ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber()),
                    KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid)))
                .conditionExpression(
                    "attribute_not_exists(#number) OR (attribute_exists(#number) AND #uuid = :uuid)")
                .expressionAttributeNames(
                    Map.of("#uuid", KEY_ACCOUNT_UUID,
                        "#number", ATTR_ACCOUNT_E164))
                .expressionAttributeValues(
                    Map.of(":uuid", AttributeValues.fromUUID(uuid)))
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build())
        .build();
  }

  public void update(Account account) throws ContestedOptimisticLockException {
    UPDATE_TIMER.record(() -> {
      UpdateItemRequest updateItemRequest;
      try {
        updateItemRequest = UpdateItemRequest.builder()
            .tableName(accountsTableName)
            .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
            .updateExpression("SET #data = :data, #cds = :cds ADD #version :version_increment")
            .conditionExpression("attribute_exists(#number) AND #version = :version")
            .expressionAttributeNames(Map.of("#number", ATTR_ACCOUNT_E164,
                "#data", ATTR_ACCOUNT_DATA,
                "#cds", ATTR_CANONICALLY_DISCOVERABLE,
                "#version", ATTR_VERSION))
            .expressionAttributeValues(Map.of(
                ":data", AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
                ":cds", AttributeValues.fromBool(account.shouldBeVisibleInDirectory()),
                ":version", AttributeValues.fromInt(account.getVersion()),
                ":version_increment", AttributeValues.fromInt(1)))
            .returnValues(ReturnValue.UPDATED_NEW)
            .build();

      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      try {
        UpdateItemResponse response = client.updateItem(updateItemRequest);

        account.setVersion(AttributeValues.getInt(response.attributes(), "V", account.getVersion() + 1));
      } catch (final TransactionConflictException e) {

        throw new ContestedOptimisticLockException();

      } catch (final ConditionalCheckFailedException e) {

        // the exception doesn’t give details about which condition failed,
        // but we can infer it was an optimistic locking failure if the UUID is known
        throw get(account.getUuid()).isPresent() ? new ContestedOptimisticLockException() : e;
      }
    });
  }

  public Optional<Account> get(String number) {
    return GET_BY_NUMBER_TIMER.record(() -> {

      final GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(phoneNumbersTableName)
          .key(Map.of(ATTR_ACCOUNT_E164, AttributeValues.fromString(number)))
          .build());

      return Optional.ofNullable(response.item())
          .map(item -> item.get(KEY_ACCOUNT_UUID))
          .map(uuid -> accountByUuid(uuid))
          .map(Accounts::fromItem);
    });
  }

  private Map<String, AttributeValue> accountByUuid(AttributeValue uuid) {
    GetItemResponse r = client.getItem(GetItemRequest.builder()
        .tableName(accountsTableName)
        .key(Map.of(KEY_ACCOUNT_UUID, uuid))
        .consistentRead(true)
        .build());
    return r.item().isEmpty() ? null : r.item();
  }

  public Optional<Account> get(UUID uuid) {
    return GET_BY_UUID_TIMER.record(() ->
        Optional.ofNullable(accountByUuid(AttributeValues.fromUUID(uuid)))
            .map(Accounts::fromItem));
  }

  public void delete(UUID uuid) {
    DELETE_TIMER.record(() -> {

      Optional<Account> maybeAccount = get(uuid);

      maybeAccount.ifPresent(account -> {

        TransactWriteItem phoneNumberDelete = TransactWriteItem.builder()
            .delete(Delete.builder()
                .tableName(phoneNumbersTableName)
                .key(Map.of(ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber())))
                .build())
            .build();

        TransactWriteItem accountDelete = TransactWriteItem.builder()
            .delete(Delete.builder()
                .tableName(accountsTableName)
                .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid)))
                .build())
            .build();

        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(phoneNumberDelete, accountDelete).build();

        client.transactWriteItems(request);
      });
    });
  }

  public AccountCrawlChunk getAllFrom(final UUID from, final int maxCount) {
    final ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
        .limit(scanPageSize)
        .exclusiveStartKey(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(from)));

    return scanForChunk(scanRequestBuilder, maxCount, GET_ALL_FROM_OFFSET_TIMER);
  }

  public AccountCrawlChunk getAllFromStart(final int maxCount) {
    final ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
        .limit(scanPageSize);

    return scanForChunk(scanRequestBuilder, maxCount, GET_ALL_FROM_START_TIMER);
  }

  private AccountCrawlChunk scanForChunk(final ScanRequest.Builder scanRequestBuilder, final int maxCount, final Timer timer) {

    scanRequestBuilder.tableName(accountsTableName);

    final List<Account> accounts = timer.record(() -> scan(scanRequestBuilder.build(), maxCount)
        .stream()
        .map(Accounts::fromItem)
        .collect(Collectors.toList()));

    return new AccountCrawlChunk(accounts, accounts.size() > 0 ? accounts.get(accounts.size() - 1).getUuid() : null);
  }

  private static String extractCancellationReasonCodes(final TransactionCanceledException exception) {
    return exception.cancellationReasons().stream()
        .map(CancellationReason::code)
        .collect(Collectors.joining(", "));
  }

  @VisibleForTesting
  static Account fromItem(Map<String, AttributeValue> item) {
    if (!item.containsKey(ATTR_ACCOUNT_DATA) ||
        !item.containsKey(ATTR_ACCOUNT_E164) ||
        // TODO: eventually require ATTR_CANONICALLY_DISCOVERABLE
        !item.containsKey(KEY_ACCOUNT_UUID)) {
      throw new RuntimeException("item missing values");
    }
    try {
      Account account = SystemMapper.getMapper().readValue(item.get(ATTR_ACCOUNT_DATA).b().asByteArray(), Account.class);
      account.setNumber(item.get(ATTR_ACCOUNT_E164).s());
      account.setUuid(UUIDUtil.fromByteBuffer(item.get(KEY_ACCOUNT_UUID).b().asByteBuffer()));
      account.setVersion(Integer.parseInt(item.get(ATTR_VERSION).n()));
      account.setCanonicallyDiscoverable(Optional.ofNullable(item.get(ATTR_CANONICALLY_DISCOVERABLE)).map(av -> av.bool()).orElse(false));

      return account;

    } catch (IOException e) {
      throw new RuntimeException("Could not read stored account data", e);
    }
  }
}
