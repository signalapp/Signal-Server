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
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * Manages a global, persistent mapping of phone numbers to phone number identifiers regardless of whether those
 * numbers/identifiers are actually associated with an account.
 */
public class PhoneNumberIdentifiers {

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  @VisibleForTesting
  static final String KEY_E164 = "P";
  @VisibleForTesting
  static final String INDEX_NAME = "pni_to_p";
  @VisibleForTesting
  static final String ATTR_PHONE_NUMBER_IDENTIFIER = "PNI";

  private static final Timer GET_PNI_TIMER = Metrics.timer(name(PhoneNumberIdentifiers.class, "get"));
  private static final Timer SET_PNI_TIMER = Metrics.timer(name(PhoneNumberIdentifiers.class, "set"));

  public PhoneNumberIdentifiers(final DynamoDbClient dynamoDbClient, final String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  /**
   * Returns the phone number identifier (PNI) associated with the given phone number.
   *
   * @param phoneNumber the phone number for which to retrieve a phone number identifier
   * @return the phone number identifier associated with the given phone number
   */
  public UUID getPhoneNumberIdentifier(final String phoneNumber) {
    final GetItemResponse response = GET_PNI_TIMER.record(() -> dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_E164, AttributeValues.fromString(phoneNumber)))
        .projectionExpression(ATTR_PHONE_NUMBER_IDENTIFIER)
        .build()));

    final UUID phoneNumberIdentifier;

    if (response.hasItem()) {
      phoneNumberIdentifier = AttributeValues.getUUID(response.item(), ATTR_PHONE_NUMBER_IDENTIFIER, null);
    } else {
      phoneNumberIdentifier = generatePhoneNumberIdentifierIfNotExists(phoneNumber);
    }

    if (phoneNumberIdentifier == null) {
      throw new RuntimeException("Could not retrieve phone number identifier from stored item");
    }

    return phoneNumberIdentifier;
  }

  public Optional<String> getPhoneNumber(final UUID phoneNumberIdentifier) {
    final QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
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
        .build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    if (response.count() > 1) {
      throw new RuntimeException(
          "Impossible result: more than one phone number returned for PNI: " + phoneNumberIdentifier);
    }

    return Optional.ofNullable(response.items().get(0).get(KEY_E164).s());
  }


  @VisibleForTesting
  UUID generatePhoneNumberIdentifierIfNotExists(final String phoneNumber) {
    final UpdateItemResponse response = SET_PNI_TIMER.record(() -> dynamoDbClient.updateItem(UpdateItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_E164, AttributeValues.fromString(phoneNumber)))
        .updateExpression("SET #pni = if_not_exists(#pni, :pni)")
        .expressionAttributeNames(Map.of("#pni", ATTR_PHONE_NUMBER_IDENTIFIER))
        .expressionAttributeValues(Map.of(":pni", AttributeValues.fromUUID(UUID.randomUUID())))
        .returnValues(ReturnValue.ALL_NEW)
        .build()));

    return AttributeValues.getUUID(response.attributes(), ATTR_PHONE_NUMBER_IDENTIFIER, null);
  }
}
