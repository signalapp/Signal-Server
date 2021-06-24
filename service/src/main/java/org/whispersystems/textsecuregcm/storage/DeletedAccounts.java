/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DeletedAccounts extends AbstractDynamoDbStore {

  // e164, primary key
  static final String KEY_ACCOUNT_E164 = "P";
  static final String ATTR_ACCOUNT_UUID = "U";
  static final String ATTR_EXPIRES = "E";
  static final String ATTR_NEEDS_CDS_RECONCILIATION = "R";

  static final Duration TIME_TO_LIVE = Duration.ofDays(30);

  private final String tableName;
  private final String needsReconciliationIndexName;

  public DeletedAccounts(final DynamoDbClient dynamoDb, final String tableName, final String needsReconciliationIndexName) {

    super(dynamoDb);
    this.tableName = tableName;
    this.needsReconciliationIndexName = needsReconciliationIndexName;
  }

  public void put(UUID uuid, String e164) {
    db().putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_ACCOUNT_E164, AttributeValues.fromString(e164),
            ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
            ATTR_EXPIRES, AttributeValues.fromLong(Instant.now().plus(TIME_TO_LIVE).getEpochSecond()),
            ATTR_NEEDS_CDS_RECONCILIATION, AttributeValues.fromInt(1)))
        .build());
  }

  public List<Pair<UUID, String>> listAccountsToReconcile(final int max) {

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

  public void markReconciled(final List<String> phoneNumbersReconciled) {

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
