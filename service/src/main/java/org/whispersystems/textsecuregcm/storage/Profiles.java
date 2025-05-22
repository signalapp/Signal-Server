/*
 * Copyright 2013 Signal Messenger, LLC
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
import org.whispersystems.textsecuregcm.util.AsyncTimerUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

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

  // User's name; byte array
  private static final String ATTR_NAME = "N";

  // Avatar path/filename; string
  private static final String ATTR_AVATAR = "A";

  // Bio/about text; byte array
  private static final String ATTR_ABOUT = "B";

  // Bio/about emoji; byte array
  private static final String ATTR_EMOJI = "E";

  // Payment address; byte array
  private static final String ATTR_PAYMENT_ADDRESS = "P";

  // Phone number sharing setting; byte array
  private static final String ATTR_PHONE_NUMBER_SHARING = "S";

  // Commitment; byte array
  private static final String ATTR_COMMITMENT = "C";

  private static final Map<String, String> UPDATE_EXPRESSION_ATTRIBUTE_NAMES = Map.of(
      "#commitment", ATTR_COMMITMENT,
      "#name", ATTR_NAME,
      "#avatar", ATTR_AVATAR,
      "#about", ATTR_ABOUT,
      "#aboutEmoji", ATTR_EMOJI,
      "#paymentAddress", ATTR_PAYMENT_ADDRESS,
      "#phoneNumberSharing", ATTR_PHONE_NUMBER_SHARING);

  private static final Timer SET_PROFILES_TIMER = Metrics.timer(name(Profiles.class, "set"));
  private static final Timer GET_PROFILE_TIMER = Metrics.timer(name(Profiles.class, "get"));
  private static final String DELETE_PROFILES_TIMER_NAME = name(Profiles.class, "delete");
  private static final String PARSE_BYTE_ARRAY_COUNTER_NAME = name(Profiles.class, "parseByteArray");

  private static final int MAX_CONCURRENCY = 32;

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
          .key(buildPrimaryKey(uuid, profile.version()))
          .updateExpression(buildUpdateExpression(profile))
          .expressionAttributeNames(UPDATE_EXPRESSION_ATTRIBUTE_NAMES)
          .expressionAttributeValues(buildUpdateExpressionAttributeValues(profile))
          .build());
    });
  }

  public CompletableFuture<Void> setAsync(final UUID uuid, final VersionedProfile profile) {
    return AsyncTimerUtil.record(SET_PROFILES_TIMER, () -> dynamoDbAsyncClient.updateItem(UpdateItemRequest.builder()
            .tableName(tableName)
            .key(buildPrimaryKey(uuid, profile.version()))
            .updateExpression(buildUpdateExpression(profile))
            .expressionAttributeNames(UPDATE_EXPRESSION_ATTRIBUTE_NAMES)
            .expressionAttributeValues(buildUpdateExpressionAttributeValues(profile))
            .build()
        ).thenRun(Util.NOOP)
    ).toCompletableFuture();
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

    if (profile.name() != null) {
      updatedAttributes.add("name");
    } else {
      deletedAttributes.add("name");
    }

    if (StringUtils.isNotBlank(profile.avatar())) {
      updatedAttributes.add("avatar");
    } else {
      deletedAttributes.add("avatar");
    }

    if (profile.about() != null) {
      updatedAttributes.add("about");
    } else {
      deletedAttributes.add("about");
    }

    if (profile.aboutEmoji() != null) {
      updatedAttributes.add("aboutEmoji");
    } else {
      deletedAttributes.add("aboutEmoji");
    }

    if (profile.paymentAddress() != null) {
      updatedAttributes.add("paymentAddress");
    } else {
      deletedAttributes.add("paymentAddress");
    }

    if (profile.phoneNumberSharing() != null) {
      updatedAttributes.add("phoneNumberSharing");
    } else {
      deletedAttributes.add("phoneNumberSharing");
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

    expressionValues.put(":commitment", AttributeValues.fromByteArray(profile.commitment()));

    if (profile.name() != null) {
      expressionValues.put(":name", AttributeValues.fromByteArray(profile.name()));
    }

    if (StringUtils.isNotBlank(profile.avatar())) {
      expressionValues.put(":avatar", AttributeValues.fromString(profile.avatar()));
    }

    if (profile.about() != null) {
      expressionValues.put(":about", AttributeValues.fromByteArray(profile.about()));
    }

    if (profile.aboutEmoji() != null) {
      expressionValues.put(":aboutEmoji", AttributeValues.fromByteArray(profile.aboutEmoji()));
    }

    if (profile.paymentAddress() != null) {
      expressionValues.put(":paymentAddress", AttributeValues.fromByteArray(profile.paymentAddress()));
    }

    if (profile.phoneNumberSharing() != null) {
      expressionValues.put(":phoneNumberSharing", AttributeValues.fromByteArray(profile.phoneNumberSharing()));
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

  public CompletableFuture<Optional<VersionedProfile>> getAsync(final UUID uuid, final String version) {
    return AsyncTimerUtil.record(GET_PROFILE_TIMER, () -> dynamoDbAsyncClient.getItem(GetItemRequest.builder()
        .tableName(tableName)
        .key(buildPrimaryKey(uuid, version))
        .consistentRead(true)
        .build())
        .thenApply(response ->
            response.hasItem() ? Optional.of(fromItem(response.item())) : Optional.<VersionedProfile>empty())
    ).toCompletableFuture();
  }

  private static VersionedProfile fromItem(final Map<String, AttributeValue> item) {
    return new VersionedProfile(
        AttributeValues.getString(item, ATTR_VERSION, null),
        getBytes(item, ATTR_NAME),
        AttributeValues.getString(item, ATTR_AVATAR, null),
        getBytes(item, ATTR_EMOJI),
        getBytes(item, ATTR_ABOUT),
        getBytes(item, ATTR_PAYMENT_ADDRESS),
        getBytes(item, ATTR_PHONE_NUMBER_SHARING),
        AttributeValues.getByteArray(item, ATTR_COMMITMENT, null));
  }

  private static byte[] getBytes(final Map<String, AttributeValue> item, final String attributeName) {
    final AttributeValue attributeValue = item.get(attributeName);

    if (attributeValue == null) {
      return null;
    }
    return AttributeValues.extractByteArray(attributeValue, PARSE_BYTE_ARRAY_COUNTER_NAME);
  }

  /**
   * Deletes all profile versions for the given UUID
   *
   * @return a list of avatar URLs to be deleted
   */
  public CompletableFuture<List<String>> deleteAll(final UUID uuid) {
    final Timer.Sample sample = Timer.start();

    final AttributeValue uuidAttributeValue = AttributeValues.fromUUID(uuid);

    return Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#uuid = :uuid")
                .expressionAttributeNames(Map.of("#uuid", KEY_ACCOUNT_UUID))
                .expressionAttributeValues(Map.of(":uuid", uuidAttributeValue))
                .projectionExpression(String.join(", ", ATTR_VERSION, ATTR_AVATAR))
                .consistentRead(true)
                .build())
            .items())
        .flatMap(item -> Mono.fromFuture(() -> dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_ACCOUNT_UUID, uuidAttributeValue,
                ATTR_VERSION, item.get(ATTR_VERSION)))
            .build()))
            .flatMap(ignored -> Mono.justOrEmpty(item.get(ATTR_AVATAR)).map(AttributeValue::s)), MAX_CONCURRENCY)
        .collectList()
        .doOnSuccess(ignored -> sample.stop(Metrics.timer(DELETE_PROFILES_TIMER_NAME, "outcome", "success")))
        .doOnError(ignored -> sample.stop(Metrics.timer(DELETE_PROFILES_TIMER_NAME, "outcome", "error")))
        .toFuture();
  }
}
