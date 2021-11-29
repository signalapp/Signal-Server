/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class RemoteConfigs {

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  // Config name; string
  static final String KEY_NAME = "N";
  // Rollout percentage; integer
  private static final String ATTR_PERCENTAGE = "P";
  // Enrolled UUIDs (ACIs); list of byte arrays
  private static final String ATTR_UUIDS = "U";
  // Default value; string
  private static final String ATTR_DEFAULT_VALUE = "D";
  // Value when enrolled; string
  private static final String ATTR_VALUE = "V";
  // Hash key; string
  private static final String ATTR_HASH_KEY = "H";

  public RemoteConfigs(final DynamoDbClient dynamoDbClient, final String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  public void set(final RemoteConfig remoteConfig) {
    final Map<String, AttributeValue> item = new HashMap<>(Map.of(
        KEY_NAME, AttributeValues.fromString(remoteConfig.getName()),
        ATTR_PERCENTAGE, AttributeValues.fromInt(remoteConfig.getPercentage())));

    if (remoteConfig.getUuids() != null && !remoteConfig.getUuids().isEmpty()) {
      final List<SdkBytes> uuidByteSets = remoteConfig.getUuids().stream()
          .map(UUIDUtil::toByteBuffer)
          .map(SdkBytes::fromByteBuffer)
          .collect(Collectors.toList());

      item.put(ATTR_UUIDS, AttributeValue.builder().bs(uuidByteSets).build());
    }

    if (remoteConfig.getDefaultValue() != null) {
      item.put(ATTR_DEFAULT_VALUE, AttributeValues.fromString(remoteConfig.getDefaultValue()));
    }

    if (remoteConfig.getValue() != null) {
      item.put(ATTR_VALUE, AttributeValues.fromString(remoteConfig.getValue()));
    }

    if (remoteConfig.getHashKey() != null) {
      item.put(ATTR_HASH_KEY, AttributeValues.fromString(remoteConfig.getHashKey()));
    }

    dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(item)
        .build());
  }

  public List<RemoteConfig> getAll() {
    return dynamoDbClient.scanPaginator(ScanRequest.builder()
            .tableName(tableName)
            .consistentRead(true)
            .build())
        .items()
        .stream()
        .map(item -> {
          final String name = AttributeValues.getString(item, KEY_NAME, null);
          final int percentage = AttributeValues.getInt(item, ATTR_PERCENTAGE, 0);
          final String defaultValue = AttributeValues.getString(item, ATTR_DEFAULT_VALUE, null);
          final String value = AttributeValues.getString(item, ATTR_VALUE, null);
          final String hashKey = AttributeValues.getString(item, ATTR_HASH_KEY, null);

          final Set<UUID> uuids;

          if (item.containsKey(ATTR_UUIDS)) {
            uuids = item.get(ATTR_UUIDS).bs().stream()
                .map(sdkBytes -> UUIDUtil.fromByteBuffer(sdkBytes.asByteBuffer()))
                .collect(Collectors.toSet());
          } else {
            uuids = Collections.emptySet();
          }

          return new RemoteConfig(name, percentage, uuids, defaultValue, value, hashKey);
        })
        .collect(Collectors.toList());
  }

  public void delete(final String name) {
    dynamoDbClient.deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_NAME, AttributeValues.fromString(name)))
        .build());
  }
}
