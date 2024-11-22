/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Manages a global, persistent mapping of phone numbers to phone number identifiers regardless of whether those
 * numbers/identifiers are actually associated with an account.
 */
public class PhoneNumberIdentifiers {

  private final DynamoDbAsyncClient dynamoDbClient;
  private final String tableName;

  @VisibleForTesting
  static final String KEY_E164 = "P";
  @VisibleForTesting
  static final String INDEX_NAME = "pni_to_p";
  @VisibleForTesting
  static final String ATTR_PHONE_NUMBER_IDENTIFIER = "PNI";

  private static final Timer GET_PNI_TIMER = Metrics.timer(name(PhoneNumberIdentifiers.class, "get"));
  private static final Timer SET_PNI_TIMER = Metrics.timer(name(PhoneNumberIdentifiers.class, "set"));

  public PhoneNumberIdentifiers(final DynamoDbAsyncClient dynamoDbClient, final String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  /**
   * Returns the phone number identifier (PNI) associated with the given phone number.
   *
   * @param phoneNumber the phone number for which to retrieve a phone number identifier
   * @return the phone number identifier associated with the given phone number
   */
  public CompletableFuture<UUID> getPhoneNumberIdentifier(final String phoneNumber) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_E164, AttributeValues.fromString(phoneNumber)))
        .projectionExpression(ATTR_PHONE_NUMBER_IDENTIFIER)
        .build())
        .thenCompose(response -> response.hasItem()
            ? CompletableFuture.completedFuture(AttributeValues.getUUID(response.item(), ATTR_PHONE_NUMBER_IDENTIFIER, null))
            : generatePhoneNumberIdentifierIfNotExists(phoneNumber))
        .whenComplete((ignored, throwable) -> sample.stop(GET_PNI_TIMER));
  }

  public CompletableFuture<Optional<String>> getPhoneNumber(final UUID phoneNumberIdentifier) {
    return dynamoDbClient.query(QueryRequest.builder()
            .tableName(tableName)
            .indexName(INDEX_NAME)
            .keyConditionExpression("#pni = :pni")
            .projectionExpression("#phone_number")
            .expressionAttributeNames(Map.of(
                "#phone_number", KEY_E164,
                "#pni", ATTR_PHONE_NUMBER_IDENTIFIER
            ))
            .expressionAttributeValues(Map.of(
                ":pni", AttributeValues.fromUUID(phoneNumberIdentifier)
            ))
            .build())
        .thenApply(response -> {
          if (response.count() == 0) {
            return Optional.empty();
          }

          if (response.count() > 1) {
            throw new RuntimeException(
                "Impossible result: more than one phone number returned for PNI: " + phoneNumberIdentifier);
          }

          return Optional.ofNullable(response.items().getFirst().get(KEY_E164).s());
        });
  }


  @VisibleForTesting
  CompletableFuture<UUID> generatePhoneNumberIdentifierIfNotExists(final String phoneNumber) {
    final Timer.Sample sample = Timer.start();

    return dynamoDbClient.updateItem(UpdateItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(KEY_E164, AttributeValues.fromString(phoneNumber)))
            .updateExpression("SET #pni = if_not_exists(#pni, :pni)")
            .expressionAttributeNames(Map.of("#pni", ATTR_PHONE_NUMBER_IDENTIFIER))
            .expressionAttributeValues(Map.of(":pni", AttributeValues.fromUUID(UUID.randomUUID())))
            .returnValues(ReturnValue.ALL_NEW)
            .build())
        .thenApply(response -> AttributeValues.getUUID(response.attributes(), ATTR_PHONE_NUMBER_IDENTIFIER, null))
        .whenComplete((ignored, throwable) -> sample.stop(SET_PNI_TIMER));
  }
}
