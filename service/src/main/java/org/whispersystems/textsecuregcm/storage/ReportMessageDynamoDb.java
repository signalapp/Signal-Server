package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class ReportMessageDynamoDb {

  static final String KEY_HASH = "H";
  static final String ATTR_TTL = "E";

  private final DynamoDbClient db;
  private final String tableName;
  private final Duration ttl;

  private static final String REMOVED_MESSAGE_COUNTER_NAME = name(ReportMessageDynamoDb.class, "removed");
  private static final Timer REMOVED_MESSAGE_AGE_TIMER = Timer
      .builder(name(ReportMessageDynamoDb.class, "removedMessageAge"))
      .publishPercentiles(0.5, 0.75, 0.95, 0.99)
      .distributionStatisticExpiry(Duration.ofDays(1))
      .register(Metrics.globalRegistry);

  public ReportMessageDynamoDb(final DynamoDbClient dynamoDB, final String tableName, final Duration ttl) {
    this.db = dynamoDB;
    this.tableName = tableName;
    this.ttl = ttl;
  }

  public void store(byte[] hash) {
    db.putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_HASH, AttributeValues.fromByteArray(hash),
            ATTR_TTL, AttributeValues.fromLong(Instant.now().plus(ttl).getEpochSecond())
        ))
        .build());
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
