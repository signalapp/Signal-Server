package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class MigrationDeletedAccounts extends AbstractDynamoDbStore {

  private final String tableName;

  static final String KEY_UUID = "U";

  public MigrationDeletedAccounts(DynamoDbClient dynamoDb, String tableName) {
    super(dynamoDb);
    this.tableName = tableName;
  }

  public void put(UUID uuid) {
    db().putItem(PutItemRequest.builder()
        .tableName(tableName)
        .item(primaryKey(uuid))
        .build());
  }

  public List<UUID> getRecentlyDeletedUuids() {

    final List<UUID> uuids = new ArrayList<>();
    Optional<ScanResponse> firstPage = db().scanPaginator(ScanRequest.builder()
        .tableName(tableName)
        .build()).stream().findAny();  // get the first available response

    if (firstPage.isPresent()) {
      for (Map<String, AttributeValue> item : firstPage.get().items()) {
        // only process one page each time. If we have a significant backlog at the end of the migration
        // we can handle it separately
        uuids.add(AttributeValues.getUUID(item, KEY_UUID, null));
      }
    }

    return uuids;
  }

  public void delete(List<UUID> uuids) {

    writeInBatches(uuids, (batch) -> {
      List<WriteRequest> deletes = batch.stream().map((uuid) -> WriteRequest.builder().deleteRequest(DeleteRequest.builder()
          .key(primaryKey(uuid))
          .build()).build()).collect(Collectors.toList());

      executeTableWriteItemsUntilComplete(Map.of(tableName, deletes));
    });
  }

  @VisibleForTesting
  public static Map<String, AttributeValue> primaryKey(UUID uuid) {
    return Map.of(KEY_UUID, AttributeValues.fromUUID(uuid));
  }

}
