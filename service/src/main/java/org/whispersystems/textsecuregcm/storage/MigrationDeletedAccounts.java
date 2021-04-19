package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class MigrationDeletedAccounts extends AbstractDynamoDbStore {

  private final Table table;

  static final String KEY_UUID = "U";

  public MigrationDeletedAccounts(DynamoDB dynamoDb, String tableName) {
    super(dynamoDb);

    table = dynamoDb.getTable(tableName);
  }

  public void put(UUID uuid) {
    table.putItem(new Item()
        .withPrimaryKey(primaryKey(uuid)));
  }

  public List<UUID> getRecentlyDeletedUuids() {

    final List<UUID> uuids = new ArrayList<>();

    for (Item item : table.scan(new ScanSpec()).firstPage()) {
      // only process one page each time. If we have a significant backlog at the end of the migration
      // we can handle it separately
      uuids.add(UUIDUtil.fromByteBuffer(item.getByteBuffer(KEY_UUID)));
    }

    return uuids;
  }

  public void delete(List<UUID> uuids) {

    writeInBatches(uuids, (batch) -> {

      final TableWriteItems deleteItems = new TableWriteItems(table.getTableName());

      for (UUID uuid : batch) {
        deleteItems.addPrimaryKeyToDelete(primaryKey(uuid));
      }

      executeTableWriteItemsUntilComplete(deleteItems);
    });
  }

  @VisibleForTesting
  public static PrimaryKey primaryKey(UUID uuid) {
    return new PrimaryKey(KEY_UUID, UUIDUtil.toBytes(uuid));
  }

}
