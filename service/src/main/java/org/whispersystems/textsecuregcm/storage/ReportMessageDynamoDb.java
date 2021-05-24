package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class ReportMessageDynamoDb {

  static final String KEY_HASH = "H";
  static final String ATTR_TTL = "E";

  static final Duration TIME_TO_LIVE = Duration.ofDays(7);

  private final DynamoDbClient db;
  private final String tableName;

  public ReportMessageDynamoDb(final DynamoDbClient dynamoDB, final String tableName) {
    this.db = dynamoDB;
    this.tableName = tableName;
  }

  public void store(byte[] hash) {
    db.putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(Map.of(
            KEY_HASH, AttributeValues.fromByteArray(hash),
            ATTR_TTL, AttributeValues.fromLong(Instant.now().plus(TIME_TO_LIVE).getEpochSecond())
        ))
        .build());
  }

  public boolean remove(byte[] hash) {
    final DeleteItemResponse deleteItemResponse = db.deleteItem(DeleteItemRequest.builder()
        .tableName(tableName)
        .key(Map.of(KEY_HASH, AttributeValues.fromByteArray(hash)))
        .returnValues(ReturnValue.ALL_OLD)
        .build());
    return !deleteItemResponse.attributes().isEmpty();
  }
}
