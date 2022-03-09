/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DeletedAccounts extends AbstractDynamoDbStore {

  // e164, primary key
  static final String KEY_ACCOUNT_E164 = "P";
  static final String ATTR_ACCOUNT_UUID = "U";
  static final String ATTR_EXPIRES = "E";
  static final String ATTR_NEEDS_CDS_RECONCILIATION = "R";

  static final String UUID_TO_E164_INDEX_NAME = "u_to_p";

  static final Duration TIME_TO_LIVE = Duration.ofDays(30);

  // Note that this limit is imposed by DynamoDB itself; going above 100 will result in errors
  static final int GET_BATCH_SIZE = 100;

  private final String tableName;
  private final String needsReconciliationIndexName;

  public DeletedAccounts(final DynamoDbClient dynamoDb, final String tableName, final String needsReconciliationIndexName) {

    super(dynamoDb);
    this.tableName = tableName;
    this.needsReconciliationIndexName = needsReconciliationIndexName;
  }

  void put(UUID uuid, String e164, boolean needsReconciliation) {
    db().putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_ACCOUNT_E164, AttributeValues.fromString(e164),
            ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
            ATTR_EXPIRES, AttributeValues.fromLong(Instant.now().plus(TIME_TO_LIVE).getEpochSecond()),
            ATTR_NEEDS_CDS_RECONCILIATION, AttributeValues.fromInt(needsReconciliation ? 1 : 0)))
        .build());
  }

  Optional<UUID> findUuid(final String e164) {
    final GetItemResponse response = db().getItem(GetItemRequest.builder()
        .tableName(tableName)
        .consistentRead(true)
        .key(Map.of(KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .build());

    return Optional.ofNullable(AttributeValues.getUUID(response.item(), ATTR_ACCOUNT_UUID, null));
  }

  Optional<String> findE164(final UUID uuid) {
    final QueryResponse response = db().query(QueryRequest.builder()
        .tableName(tableName)
        .indexName(UUID_TO_E164_INDEX_NAME)
        .keyConditionExpression("#uuid = :uuid")
        .projectionExpression("#e164")
        .expressionAttributeNames(Map.of("#uuid", ATTR_ACCOUNT_UUID,
            "#e164", KEY_ACCOUNT_E164))
        .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(uuid))).build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    if (response.count() > 1) {
      throw new RuntimeException(
          "Impossible result: more than one phone number returned for UUID: " + uuid);
    }

    return Optional.ofNullable(response.items().get(0).get(KEY_ACCOUNT_E164).s());
  }

  void remove(final String e164) {
    db().deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .build());
  }

  List<Pair<UUID, String>> listAccountsToReconcile(final int max) {

    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(tableName)
        .indexName(needsReconciliationIndexName)
        .limit(max)
        .build();

    return scan(scanRequest, max)
        .stream()
        .map(item -> new Pair<>(
            AttributeValues.getUUID(item, ATTR_ACCOUNT_UUID, null),
            AttributeValues.getString(item, KEY_ACCOUNT_E164, null)))
        .collect(Collectors.toList());
  }

  Set<String> getAccountsNeedingReconciliation(final Collection<String> e164s) {
    final Queue<Map<String, AttributeValue>> pendingKeys = e164s.stream()
        .map(e164 -> Map.of(KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .collect(Collectors.toCollection(() -> new ArrayDeque<>(e164s.size())));

    final Set<String> accountsNeedingReconciliation = new HashSet<>(e164s.size());
    final List<Map<String, AttributeValue>> batchKeys = new ArrayList<>(GET_BATCH_SIZE);

    while (!pendingKeys.isEmpty()) {
      batchKeys.clear();

      for (int i = 0; i < GET_BATCH_SIZE && !pendingKeys.isEmpty(); i++) {
        batchKeys.add(pendingKeys.remove());
      }

      final BatchGetItemResponse response = db().batchGetItem(BatchGetItemRequest.builder()
          .requestItems(Map.of(tableName, KeysAndAttributes.builder()
              .consistentRead(true)
              .keys(batchKeys)
              .build()))
          .build());

      response.responses().getOrDefault(tableName, Collections.emptyList()).stream()
          .filter(attributes -> AttributeValues.getInt(attributes, ATTR_NEEDS_CDS_RECONCILIATION, 0) == 1)
          .map(attributes -> AttributeValues.getString(attributes, KEY_ACCOUNT_E164, null))
          .forEach(accountsNeedingReconciliation::add);

      if (response.hasUnprocessedKeys() && response.unprocessedKeys().containsKey(tableName)) {
        pendingKeys.addAll(response.unprocessedKeys().get(tableName).keys());
      }
    }

    return accountsNeedingReconciliation;
  }

  void markReconciled(final Collection<String> phoneNumbersReconciled) {

    phoneNumbersReconciled.forEach(number -> db().updateItem(
        UpdateItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_E164, AttributeValues.fromString(number)
            ))
            .updateExpression("REMOVE #needs_reconciliation")
            .expressionAttributeNames(Map.of(
                "#needs_reconciliation", ATTR_NEEDS_CDS_RECONCILIATION
            ))
            .build()
    ));
  }

}
