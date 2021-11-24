/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;

public class Profiles {

  private final DynamoDbClient dynamoDbClient;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  // UUID of the account that owns this profile; byte array
  @VisibleForTesting
  static final String KEY_ACCOUNT_UUID = "U";

  // Version of this profile; string
  @VisibleForTesting
  static final String ATTR_VERSION = "V";

  // User's name; string
  private static final String ATTR_NAME = "N";

  // Avatar path/filename; string
  private static final String ATTR_AVATAR = "A";

  // Bio/about text; string
  private static final String ATTR_ABOUT = "B";

  // Bio/about emoji; string
  private static final String ATTR_EMOJI = "E";

  // Payment address; string
  private static final String ATTR_PAYMENT_ADDRESS = "P";

  // Commitment; byte array
  private static final String ATTR_COMMITMENT = "C";

  private static final Map<String, String> UPDATE_EXPRESSION_ATTRIBUTE_NAMES = Map.of(
      "#commitment", ATTR_COMMITMENT,
      "#name", ATTR_NAME,
      "#avatar", ATTR_AVATAR,
      "#about", ATTR_ABOUT,
      "#aboutEmoji", ATTR_EMOJI,
      "#paymentAddress", ATTR_PAYMENT_ADDRESS);

  private static final Timer SET_PROFILES_TIMER = Metrics.timer(name(Profiles.class, "set"));
  private static final Timer GET_PROFILE_TIMER = Metrics.timer(name(Profiles.class, "get"));
  private static final Timer DELETE_PROFILES_TIMER = Metrics.timer(name(Profiles.class, "delete"));

  public Profiles(final DynamoDbClient dynamoDbClient,
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String tableName) {

    this.dynamoDbClient = dynamoDbClient;
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  public void set(final UUID uuid, final VersionedProfile profile) {
    SET_PROFILES_TIMER.record(() -> {
      dynamoDbClient.updateItem(UpdateItemRequest.builder()
          .tableName(tableName)
          .key(buildPrimaryKey(uuid, profile.getVersion()))
          .updateExpression(buildUpdateExpression(profile))
          .expressionAttributeNames(UPDATE_EXPRESSION_ATTRIBUTE_NAMES)
          .expressionAttributeValues(buildUpdateExpressionAttributeValues(profile))
          .build());
    });
  }

  private static Map<String, AttributeValue> buildPrimaryKey(final UUID uuid, final String version) {
    return Map.of(
        KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
        ATTR_VERSION, AttributeValues.fromString(version));
  }

  @VisibleForTesting
  static String buildUpdateExpression(final VersionedProfile profile) {
    final List<String> updatedAttributes = new ArrayList<>(5);
    final List<String> deletedAttributes = new ArrayList<>(5);

    if (StringUtils.isNotBlank(profile.getName())) {
      updatedAttributes.add("name");
    } else {
      deletedAttributes.add("name");
    }

    if (StringUtils.isNotBlank(profile.getAvatar())) {
      updatedAttributes.add("avatar");
    } else {
      deletedAttributes.add("avatar");
    }

    if (StringUtils.isNotBlank(profile.getAbout())) {
      updatedAttributes.add("about");
    } else {
      deletedAttributes.add("about");
    }

    if (StringUtils.isNotBlank(profile.getAboutEmoji())) {
      updatedAttributes.add("aboutEmoji");
    } else {
      deletedAttributes.add("aboutEmoji");
    }

    if (StringUtils.isNotBlank(profile.getPaymentAddress())) {
      updatedAttributes.add("paymentAddress");
    } else {
      deletedAttributes.add("paymentAddress");
    }

    final StringBuilder updateExpressionBuilder = new StringBuilder(
        "SET #commitment = if_not_exists(#commitment, :commitment)");

    if (!updatedAttributes.isEmpty()) {
      updatedAttributes.forEach(token -> updateExpressionBuilder
          .append(", #")
          .append(token)
          .append(" = :")
          .append(token));
    }

    if (!deletedAttributes.isEmpty()) {
      updateExpressionBuilder.append(" REMOVE ");
      updateExpressionBuilder.append(deletedAttributes.stream()
          .map(token -> "#" + token)
          .collect(Collectors.joining(", ")));
    }

    return updateExpressionBuilder.toString();
  }

  @VisibleForTesting
  static Map<String, AttributeValue> buildUpdateExpressionAttributeValues(final VersionedProfile profile) {
    final Map<String, AttributeValue> expressionValues = new HashMap<>();
    
    expressionValues.put(":commitment", AttributeValues.fromByteArray(profile.getCommitment()));

    if (StringUtils.isNotBlank(profile.getName())) {
      expressionValues.put(":name", AttributeValues.fromString(profile.getName()));
    }

    if (StringUtils.isNotBlank(profile.getAvatar())) {
      expressionValues.put(":avatar", AttributeValues.fromString(profile.getAvatar()));
    }

    if (StringUtils.isNotBlank(profile.getAbout())) {
      expressionValues.put(":about", AttributeValues.fromString(profile.getAbout()));
    }

    if (StringUtils.isNotBlank(profile.getAboutEmoji())) {
      expressionValues.put(":aboutEmoji", AttributeValues.fromString(profile.getAboutEmoji()));
    }

    if (StringUtils.isNotBlank(profile.getPaymentAddress())) {
      expressionValues.put(":paymentAddress", AttributeValues.fromString(profile.getPaymentAddress()));
    }
    
    return expressionValues;
  }

  public Optional<VersionedProfile> get(final UUID uuid, final String version) {
    return GET_PROFILE_TIMER.record(() -> {
      final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
          .tableName(tableName)
          .key(buildPrimaryKey(uuid, version))
          .consistentRead(true)
          .build());

      return response.hasItem() ? Optional.of(fromItem(response.item())) : Optional.empty();
    });
  }

  private static VersionedProfile fromItem(final Map<String, AttributeValue> item) {
    return new VersionedProfile(
        AttributeValues.getString(item, ATTR_VERSION, null),
        AttributeValues.getString(item, ATTR_NAME, null),
        AttributeValues.getString(item, ATTR_AVATAR, null),
        AttributeValues.getString(item, ATTR_EMOJI, null),
        AttributeValues.getString(item, ATTR_ABOUT, null),
        AttributeValues.getString(item, ATTR_PAYMENT_ADDRESS, null),
        AttributeValues.getByteArray(item, ATTR_COMMITMENT, null));
  }

  public void deleteAll(final UUID uuid) {
    DELETE_PROFILES_TIMER.record(() -> {
      final AttributeValue uuidAttributeValue = AttributeValues.fromUUID(uuid);

      final QueryIterable queryIterable = dynamoDbClient.queryPaginator(QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("#uuid = :uuid")
          .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
          .expressionAttributeValues(Map.of(":uuid", uuidAttributeValue))
          .projectionExpression(ATTR_VERSION)
          .consistentRead(true)
          .build());

      CompletableFuture.allOf(queryIterable.items().stream()
          .map(item -> dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
              .tableName(tableName)
              .key(Map.of(
                  KEY_ACCOUNT_UUID, uuidAttributeValue,
                  ATTR_VERSION, item.get(ATTR_VERSION)))
              .build()))
          .toArray(CompletableFuture[]::new)).join();
    });
  }
}
