package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import java.time.Duration;
import java.time.Instant;

public class ReportMessageDynamoDb {

  static final String KEY_HASH = "H";
  static final String ATTR_TTL = "E";

  static final Duration TIME_TO_LIVE = Duration.ofDays(7);

  private final Table table;

  public ReportMessageDynamoDb(final DynamoDB dynamoDB, final String tableName) {

    this.table = dynamoDB.getTable(tableName);
  }

  public void store(byte[] hash) {

    table.putItem(buildItemForHash(hash));
  }

  private Item buildItemForHash(byte[] hash) {
    return new Item()
        .withBinary(KEY_HASH, hash)
        .withLong(ATTR_TTL, Instant.now().plus(TIME_TO_LIVE).getEpochSecond());
  }

  public boolean remove(byte[] hash) {

    final DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
        .withPrimaryKey(KEY_HASH, hash)
        .withReturnValues(ReturnValue.ALL_OLD);

    final DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

    return outcome.getItem() != null;
  }

}
