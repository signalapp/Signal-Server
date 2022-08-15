/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

public class ReservedUsernames {

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  private final LoadingCache<String, Pattern> patternCache = CacheBuilder.newBuilder()
      .maximumSize(1_000)
      .build(new CacheLoader<>() {
        @Override
        public Pattern load(final String s) {
          return Pattern.compile(s, Pattern.CASE_INSENSITIVE);
        }
      });

  @VisibleForTesting
  static final String KEY_PATTERN = "P";
  private static final String ATTR_RESERVED_FOR_UUID = "U";

  private static final Timer IS_RESERVED_TIMER = Metrics.timer(name(ReservedUsernames.class, "isReserved"));

  private static final Logger log = LoggerFactory.getLogger(ReservedUsernames.class);

  public ReservedUsernames(final DynamoDbClient dynamoDbClient, final String tableName) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  public boolean isReserved(final String nickname, final UUID accountIdentifier) {
    return IS_RESERVED_TIMER.record(() -> {
      final ScanIterable scanIterable = dynamoDbClient.scanPaginator(ScanRequest.builder()
          .tableName(tableName)
          .build());

      for (final ScanResponse scanResponse : scanIterable) {
        if (scanResponse.hasItems()) {
          for (final Map<String, AttributeValue> item : scanResponse.items()) {
            try {
              final Pattern pattern = patternCache.get(item.get(KEY_PATTERN).s());
              final UUID reservedFor = AttributeValues.getUUID(item, ATTR_RESERVED_FOR_UUID, null);

              if (pattern.matcher(nickname).matches() && !accountIdentifier.equals(reservedFor)) {
                return true;
              }
            } catch (final Exception e) {
              log.error("Failed to load pattern from item: {}", item, e);
            }
          }
        }
      }

      return false;
    });
  }

  public void reserveUsername(final String pattern, final UUID reservedFor) {
    dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_PATTERN, AttributeValues.fromString(pattern),
            ATTR_RESERVED_FOR_UUID, AttributeValues.fromUUID(reservedFor)))
        .build());
  }
}
