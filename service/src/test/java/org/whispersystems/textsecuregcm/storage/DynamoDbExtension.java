/*
 * Copyright 2021-2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.whispersystems.textsecuregcm.util.TestcontainersImages;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

public class DynamoDbExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ExtensionContext.Store.CloseableResource {

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

  private static final Logger logger = LoggerFactory.getLogger(DynamoDbExtension.class);

  static final ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT = ProvisionedThroughput.builder()
      .readCapacityUnits(20L)
      .writeCapacityUnits(20L)
      .build();

  private static final DockerImageName DYNAMO_DB_IMAGE = DockerImageName.parse(TestcontainersImages.getDynamoDb());
  private static final int CONTAINER_PORT = 8000;
  private static final GenericContainer<?> dynamoDbContainer =  new GenericContainer<>(DYNAMO_DB_IMAGE)
      .withExposedPorts(CONTAINER_PORT)
      .withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry");

  // These are static to simplify configuration in WhisperServerServiceTest
  private static String endpointOverride;
  private static Region region = Region.of("local");
  private static AwsCredentialsProvider awsCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
  private static DynamoDbClient dynamoDb;
  private static DynamoDbAsyncClient dynamoDbAsync;

  private final List<TableSchema> schemas;

  public DynamoDbExtension(TableSchema... schemas) {
    this.schemas = List.of(schemas);
  }

  public void setEndpointOverride(String endpointOverride) {
    DynamoDbExtension.endpointOverride = endpointOverride;
  }

  public void setRegion(String region) {
    DynamoDbExtension.region = Region.of(region);
  }

  public void setAwsCredentialsProvider(AwsCredentialsProvider awsCredentialsProvider) {
    DynamoDbExtension.awsCredentialsProvider = awsCredentialsProvider;
  }

  /**
   * Starts the DynamoDB server
   */
  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    startServer();
  }

  /**
   * Creates the tables from {@link #schemas}
   */
  @Override
  public void beforeEach(final ExtensionContext context) throws Exception {
    createTables();
  }

  /**
   * Deletes the tables from {@link #schemas}
   */
  @Override
  public void afterEach(ExtensionContext context) {
    final Instant timeout = Instant.now().plus(Duration.ofSeconds(1));

    schemas.stream().map(tableSchema -> dynamoDb.deleteTable(builder -> builder.tableName(tableSchema.tableName())))
        .forEach(deleteTableResponse -> {
          while (Instant.now().isBefore(timeout)) {
            try {
              // `deleteTable` is technically asynchronous, although it seems to be uncommon with DynamoDB Local,
              // so this will usually throw and very rarely sleep().
              dynamoDb.describeTable(builder -> builder.tableName(deleteTableResponse.tableDescription().tableName()));
              Thread.sleep(50);
            } catch (ResourceNotFoundException ignored) {
              // success
              break;
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }

  @Override
  public void close() throws Throwable {
    stopServer();
  }

  private void startServer() {
    if (endpointOverride == null) {
      dynamoDbContainer.start();
    }
    if (dynamoDbAsync == null || dynamoDb == null) {
      initializeClient();
    }
  }

  private void stopServer() {
    try {
      if (dynamoDbContainer != null) {
        dynamoDb.close();
        dynamoDb = null;

        dynamoDbAsync.close();
        dynamoDbAsync = null;

        dynamoDbContainer.stop();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * For use in integration tests that want to test resiliency/error handling
   */
  public void resetServer() {
    stopServer();
    startServer();
    createTables();
  }

  private void createTables() {
    logger.debug("Creating tables");
    schemas.forEach(this::createTable);
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
    final URI endpoint = endpointOverride == null ?
        URI.create(String.format("http://%s:%d", dynamoDbContainer.getHost(), dynamoDbContainer.getMappedPort(CONTAINER_PORT)))
        : URI.create(endpointOverride);

    dynamoDb = DynamoDbClient.builder()
        .region(region)
        .credentialsProvider(awsCredentialsProvider)
        .endpointOverride(endpoint)
        .build();
    dynamoDbAsync = DynamoDbAsyncClient.builder()
        .region(region)
        .credentialsProvider(awsCredentialsProvider)
        .endpointOverride(endpoint)
        .build();
  }

  public DynamoDbClient getDynamoDbClient() {
    return dynamoDb;
  }

  public DynamoDbAsyncClient getDynamoDbAsyncClient() {
    return dynamoDbAsync;
  }

}
