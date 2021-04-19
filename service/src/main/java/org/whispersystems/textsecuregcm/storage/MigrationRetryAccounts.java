package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class MigrationRetryAccounts extends AbstractDynamoDbStore {

  private final Table table;

  static final String KEY_UUID = "U";

  public MigrationRetryAccounts(DynamoDB dynamoDb, String tableName) {
    super(dynamoDb);

    table = dynamoDb.getTable(tableName);
  }

  public void put(UUID uuid) {
    table.putItem(new Item()
        .withPrimaryKey(primaryKey(uuid)));
  }

  public List<UUID> getUuids(int max) {

    final List<UUID> uuids = new ArrayList<>();

    for (Page<Item, ScanOutcome> page : table.scan(new ScanSpec()).pages()) {

      for (Item item : page) {
        uuids.add(UUIDUtil.fromByteBuffer(item.getByteBuffer(KEY_UUID)));

        if (uuids.size() >= max) {
          break;
        }
      }

      if (uuids.size() >= max) {
        break;
      }
    }

    return uuids;
  }

  @VisibleForTesting
  public static PrimaryKey primaryKey(UUID uuid) {
    return new PrimaryKey(KEY_UUID, UUIDUtil.toBytes(uuid));
  }

}
