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
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DeletedAccounts extends AbstractDynamoDbStore {

  // e164, primary key
  static final String KEY_ACCOUNT_E164 = "P";
  static final String ATTR_ACCOUNT_UUID = "U";
  static final String ATTR_EXPIRES = "E";
  static final String ATTR_RECONCILED_IN_CDS = "R";

  static final Duration TIME_TO_LIVE = Duration.ofDays(30);

  private final String tableName;

  public DeletedAccounts(final DynamoDbClient dynamoDb, final String tableName) {

    super(dynamoDb);
    this.tableName = tableName;
  }

  public void put(UUID uuid, String e164) {
    db().putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_ACCOUNT_E164, AttributeValues.fromString(e164),
            ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
            ATTR_EXPIRES, AttributeValues.fromLong(Instant.now().plus(TIME_TO_LIVE).getEpochSecond()),
            ATTR_RECONCILED_IN_CDS, AttributeValues.fromBoolean(false)))
        .build());
  }

  public List<Pair<UUID, String>> listAccountsToReconcile(final int max) {

    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(tableName)
        .filterExpression("#reconciled = :reconciled")
        .expressionAttributeNames(Map.of(
            "#reconciled", ATTR_RECONCILED_IN_CDS
        ))
        .expressionAttributeValues(Map.of(
            ":reconciled", AttributeValues.fromBoolean(false)
        ))
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
    writeInBatches(phoneNumbersReconciled, phoneNumbers -> {

      final List<WriteRequest> updates = phoneNumbers.stream()
          .map(phoneNumber -> WriteRequest.builder()
              .putRequest(PutRequest.builder().item(
                  Map.of(KEY_ACCOUNT_E164, AttributeValues.fromString(phoneNumber),
                      ATTR_RECONCILED_IN_CDS, AttributeValues.fromBoolean(true))
              ).build()).build()
          ).collect(Collectors.toList());

      executeTableWriteItemsUntilComplete(Map.of(tableName, updates));
    });
  }

}
