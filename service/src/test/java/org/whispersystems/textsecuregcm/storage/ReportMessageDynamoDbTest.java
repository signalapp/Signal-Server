package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class ReportMessageDynamoDbTest {

  private ReportMessageDynamoDb reportMessageDynamoDb;

  private static final String TABLE_NAME = "report_message_test";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(TABLE_NAME)
      .hashKey(ReportMessageDynamoDb.KEY_HASH)
      .attributeDefinition(new AttributeDefinition(ReportMessageDynamoDb.KEY_HASH, ScalarAttributeType.B))
      .build();


  @BeforeEach
  void setUp() {
    this.reportMessageDynamoDb = new ReportMessageDynamoDb(dynamoDbExtension.getDynamoDB(), TABLE_NAME);
  }

  @Test
  void testStore() {

    final byte[] hash1 = UUIDUtil.toBytes(UUID.randomUUID());
    final byte[] hash2 = UUIDUtil.toBytes(UUID.randomUUID());

    assertAll("database should be empty",
        () -> assertFalse(reportMessageDynamoDb.remove(hash1)),
        () -> assertFalse(reportMessageDynamoDb.remove(hash2))
    );

    reportMessageDynamoDb.store(hash1);
    reportMessageDynamoDb.store(hash2);

    assertAll("both hashes should be found",
        () -> assertTrue(reportMessageDynamoDb.remove(hash1)),
        () -> assertTrue(reportMessageDynamoDb.remove(hash2))
    );

    assertAll( "database should be empty",
        () -> assertFalse(reportMessageDynamoDb.remove(hash1)),
        () -> assertFalse(reportMessageDynamoDb.remove(hash2))
    );
  }

}
