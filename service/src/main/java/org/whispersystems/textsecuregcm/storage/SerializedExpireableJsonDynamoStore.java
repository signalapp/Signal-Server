/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public abstract class SerializedExpireableJsonDynamoStore<T> {

  public interface Expireable {

    @JsonIgnore
    long getExpirationEpochSeconds();
  }

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;
  private final Clock clock;
  private final Class<T> deserializationTargetClass;

  @VisibleForTesting
  static final String KEY_KEY = "K";

  private static final String ATTR_SERIALIZED_VALUE = "V";
  private static final String ATTR_TTL = "E";

  private final Logger log = LoggerFactory.getLogger(getClass());

  public SerializedExpireableJsonDynamoStore(final DynamoDbClient dynamoDbClient, final String tableName,
      final Clock clock) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
    this.clock = clock;

    if (getClass().getGenericSuperclass() instanceof ParameterizedType pt) {
      // Extract the parameterized class declared by concrete implementations, so that it can
      // be passed to future deserialization calls
      final Type[] actualTypeArguments = pt.getActualTypeArguments();
      if (actualTypeArguments.length != 1) {
        throw new RuntimeException("Unexpected number of type arguments: " + actualTypeArguments.length);
      }
      deserializationTargetClass = (Class<T>) actualTypeArguments[0];
    } else {
      throw new RuntimeException(
          "Unable to determine target class for deserialization - generic superclass is not a ParameterizedType");
    }
  }

  public void insert(final String key, final T v) {
    put(key, v, builder -> builder.expressionAttributeNames(Map.of("#key", KEY_KEY))
        .conditionExpression("attribute_not_exists(#key)"));
  }

  public void update(final String key, final T v) {
    put(key, v, _ -> {});
  }

  private void put(final String key, final T v,
      final Consumer<PutItemRequest.Builder> putRequestCustomizer) {
    try {
      final Map<String, AttributeValue> attributeValueMap = new HashMap<>(Map.of(
          KEY_KEY, AttributeValues.fromString(key),
          ATTR_SERIALIZED_VALUE,
          AttributeValues.fromString(SystemMapper.jsonMapper().writeValueAsString(v))));
      if (v instanceof Expireable ev) {
        attributeValueMap.put(ATTR_TTL, AttributeValues.fromLong(getExpirationTimestamp(ev)));
      }
      final PutItemRequest.Builder builder = PutItemRequest.builder()
          .tableName(tableName)
          .item(attributeValueMap);
      putRequestCustomizer.accept(builder);

      dynamoDbClient.putItem(builder.build());
    } catch (final JsonProcessingException e) {
      // This should never happen when writing directly to a string except in cases of serious misconfiguration, which
      // would be caught by tests.
      throw new AssertionError(e);
    }
  }

  private long getExpirationTimestamp(final Expireable v) {
    return v.getExpirationEpochSeconds();
  }

  public Optional<T> findForKey(final String key) {
    final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .consistentRead(true)
            .key(Map.of(KEY_KEY, AttributeValues.fromString(key)))
            .build());

    try {
      return response.hasItem()
          ? filterMaybeExpiredValue(
          SystemMapper.jsonMapper()
              .readValue(response.item().get(ATTR_SERIALIZED_VALUE).s(), deserializationTargetClass))
          : Optional.empty();
    } catch (final JsonProcessingException e) {
      log.error("Failed to parse stored value", e);
      return Optional.empty();
    }
  }

  private Optional<T> filterMaybeExpiredValue(T v) {
    // It's possible for DynamoDB to return items after their expiration time (although it is very unlikely for small
    // tables)
    if (v instanceof Expireable ev) {
      if (getExpirationTimestamp(ev) < clock.instant().getEpochSecond()) {
        return Optional.empty();
      }
    }

    return Optional.of(v);
  }

  public void remove(final String key) {
    dynamoDbClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_KEY, AttributeValues.fromString(key)))
            .build());
  }

}
