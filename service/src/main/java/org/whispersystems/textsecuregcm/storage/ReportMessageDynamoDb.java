package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class ReportMessageDynamoDb {

  static final String KEY_HASH = "H";
  static final String ATTR_TTL = "E";

  private final DynamoDbClient db;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;
  private final Duration ttl;

  private static final String REMOVED_MESSAGE_COUNTER_NAME = name(ReportMessageDynamoDb.class, "removed");
  private static final Timer REMOVED_MESSAGE_AGE_TIMER = Timer
      .builder(name(ReportMessageDynamoDb.class, "removedMessageAge"))
      .publishPercentiles(0.5, 0.75, 0.95, 0.99)
      .distributionStatisticExpiry(Duration.ofDays(1))
      .register(Metrics.globalRegistry);

  public ReportMessageDynamoDb(final DynamoDbClient dynamoDB,
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String tableName,
      final Duration ttl) {

    this.db = dynamoDB;
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
    this.ttl = ttl;
  }

  public CompletableFuture<Void> store(byte[] hash) {
    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_HASH, AttributeValues.fromByteArray(hash),
            ATTR_TTL, AttributeValues.fromLong(Instant.now().plus(ttl).getEpochSecond())
        ))
        .build())
        .thenRun(Util.NOOP);
  }

  public boolean remove(byte[] hash) {
    final DeleteItemResponse deleteItemResponse = db.deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_HASH, AttributeValues.fromByteArray(hash)))
        .returnValues(ReturnValue.ALL_OLD)
        .build());

    final boolean found = !deleteItemResponse.attributes().isEmpty();

    if (found) {
      if (deleteItemResponse.attributes().containsKey(ATTR_TTL)) {
        final Instant expiration =
            Instant.ofEpochSecond(Long.parseLong(deleteItemResponse.attributes().get(ATTR_TTL).n()));

        final Duration approximateAge = ttl.minus(Duration.between(Instant.now(), expiration));

        REMOVED_MESSAGE_AGE_TIMER.record(approximateAge);
      }
    }

    Metrics.counter(REMOVED_MESSAGE_COUNTER_NAME, "found", String.valueOf(found)).increment();

    return found;
  }
}
