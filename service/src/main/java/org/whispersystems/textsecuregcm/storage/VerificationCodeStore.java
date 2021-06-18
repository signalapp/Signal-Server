/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

public class VerificationCodeStore {

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  private final Timer insertTimer;
  private final Timer getTimer;
  private final Timer removeTimer;

  @VisibleForTesting
  static final String KEY_E164 = "P";

  private static final String ATTR_STORED_CODE = "C";
  private static final String ATTR_TTL = "E";

  private static final Logger log = LoggerFactory.getLogger(VerificationCodeStore.class);

  public VerificationCodeStore(final DynamoDbClient dynamoDbClient, final String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;

    this.insertTimer = Metrics.timer(name(getClass(), "insert"), "table", tableName);
    this.getTimer = Metrics.timer(name(getClass(), "get"), "table", tableName);
    this.removeTimer = Metrics.timer(name(getClass(), "remove"), "table", tableName);
  }

  public void insert(final String number, final StoredVerificationCode verificationCode) {
    insertTimer.record(() -> {
      try {
        dynamoDbClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                KEY_E164, AttributeValues.fromString(number),
                ATTR_STORED_CODE, AttributeValues.fromString(SystemMapper.getMapper().writeValueAsString(verificationCode)),
                ATTR_TTL, AttributeValues.fromLong(getExpirationTimestamp(verificationCode))))
            .build());
      } catch (final JsonProcessingException e) {
        // This should never happen when writing directly to a string except in cases of serious misconfiguration, which
        // would be caught by tests.
        throw new AssertionError(e);
      }
    });
  }

  private long getExpirationTimestamp(final StoredVerificationCode storedVerificationCode) {
    return Instant.ofEpochMilli(storedVerificationCode.getTimestamp()).plus(StoredVerificationCode.EXPIRATION).getEpochSecond();
  }

  public Optional<StoredVerificationCode> findForNumber(final String number) {
    return getTimer.record(() -> {
      final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
          .tableName(tableName)
          .consistentRead(true)
          .key(Map.of(KEY_E164, AttributeValues.fromString(number)))
          .build());

      try {
        return response.hasItem()
            ? Optional.of(SystemMapper.getMapper().readValue(response.item().get(ATTR_STORED_CODE).s(), StoredVerificationCode.class))
            : Optional.empty();
      } catch (final JsonProcessingException e) {
        log.error("Failed to parse stored verification code", e);
        return Optional.empty();
      }
    });
  }

  public void remove(final String number) {
    removeTimer.record(() -> {
      dynamoDbClient.deleteItem(DeleteItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(KEY_E164, AttributeValues.fromString(number)))
          .build());
    });
  }
}
