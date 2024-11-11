/*
 * Copyright 2021-2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

public class DynamoDbExtension implements BeforeEachCallback, AfterEachCallback {

  public interface TableSchema {
    String tableName();
    String hashKeyName();
    String rangeKeyName();
    List<AttributeDefinition> attributeDefinitions();
    List<GlobalSecondaryIndex> globalSecondaryIndexes();
    List<LocalSecondaryIndex> localSecondaryIndexes();
  }

  record RawSchema(
    String tableName,
    String hashKeyName,
    String rangeKeyName,
    List<AttributeDefinition> attributeDefinitions,
    List<GlobalSecondaryIndex> globalSecondaryIndexes,
    List<LocalSecondaryIndex> localSecondaryIndexes
  ) implements TableSchema { }

  static final ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT = ProvisionedThroughput.builder()
      .readCapacityUnits(20L)
      .writeCapacityUnits(20L)
      .build();

  private AmazonDynamoDBLocal embedded;
  private final List<TableSchema> schemas;
  private DynamoDbClient dynamoDB2;
  private DynamoDbAsyncClient dynamoAsyncDB2;

  public DynamoDbExtension(TableSchema... schemas) {
    this.schemas = List.of(schemas);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    stopServer();
  }

  /**
   * For use in integration tests that want to test resiliency/error handling
   */
  public void stopServer() {
    try {
      embedded.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    initializeClient();

    createTables();
  }

  private void createTables() {
    schemas.stream().forEach(this::createTable);
  }

  private void createTable(TableSchema schema) {
    KeySchemaElement[] keySchemaElements;
    if (schema.rangeKeyName() == null) {
      keySchemaElements = new KeySchemaElement[] {
          KeySchemaElement.builder().attributeName(schema.hashKeyName()).keyType(KeyType.HASH).build(),
      };
    } else {
      keySchemaElements = new KeySchemaElement[] {
          KeySchemaElement.builder().attributeName(schema.hashKeyName()).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(schema.rangeKeyName()).keyType(KeyType.RANGE).build(),
      };
    }

    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName(schema.tableName())
        .keySchema(keySchemaElements)
        .attributeDefinitions(schema.attributeDefinitions().isEmpty() ? null : schema.attributeDefinitions())
        .globalSecondaryIndexes(schema.globalSecondaryIndexes().isEmpty() ? null : schema.globalSecondaryIndexes())
        .localSecondaryIndexes(schema.localSecondaryIndexes().isEmpty() ? null : schema.localSecondaryIndexes())
        .provisionedThroughput(DEFAULT_PROVISIONED_THROUGHPUT)
        .build();

    getDynamoDbClient().createTable(createTableRequest);
  }

  private void initializeClient() {
    embedded = DynamoDBEmbedded.create();
    dynamoDB2 = embedded.dynamoDbClient();
    dynamoAsyncDB2 = embedded.dynamoDbAsyncClient();
  }

  public DynamoDbClient getDynamoDbClient() {
    return dynamoDB2;
  }

  public DynamoDbAsyncClient getDynamoDbAsyncClient() {
    return dynamoAsyncDB2;
  }

}
