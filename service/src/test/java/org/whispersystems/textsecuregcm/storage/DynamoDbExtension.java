package org.whispersystems.textsecuregcm.storage;

import com.almworks.sqlite4java.SQLite;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
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

public class DynamoDbExtension implements BeforeEachCallback, AfterEachCallback {

  static final String DEFAULT_TABLE_NAME = "test_table";

  static final ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT = ProvisionedThroughput.builder()
      .readCapacityUnits(20L)
      .writeCapacityUnits(20L)
      .build();

  private static final AtomicBoolean libraryLoaded = new AtomicBoolean();

  private DynamoDBProxyServer server;
  private int port;

  private final String tableName;
  private final String hashKeyName;
  private final String rangeKeyName;

  private final List<AttributeDefinition> attributeDefinitions;
  private final List<GlobalSecondaryIndex> globalSecondaryIndexes;
  private final List<LocalSecondaryIndex> localSecondaryIndexes;

  private final long readCapacityUnits;
  private final long writeCapacityUnits;

  private DynamoDbClient dynamoDB2;
  private DynamoDbAsyncClient dynamoAsyncDB2;
  private AmazonDynamoDB legacyDynamoClient;

  private DynamoDbExtension(String tableName, String hashKey, String rangeKey,
      List<AttributeDefinition> attributeDefinitions, List<GlobalSecondaryIndex> globalSecondaryIndexes,
      final List<LocalSecondaryIndex> localSecondaryIndexes,
      long readCapacityUnits,
      long writeCapacityUnits) {

    this.tableName = tableName;
    this.hashKeyName = hashKey;
    this.rangeKeyName = rangeKey;
    this.localSecondaryIndexes = localSecondaryIndexes;

    this.readCapacityUnits = readCapacityUnits;
    this.writeCapacityUnits = writeCapacityUnits;

    this.attributeDefinitions = attributeDefinitions;
    this.globalSecondaryIndexes = globalSecondaryIndexes;
  }

  public static DynamoDbExtensionBuilder builder() {
    return new DynamoDbExtensionBuilder();
  }

  private static void loadLibrary() {
    // to avoid noise in the logs from “library already loaded” warnings, we make sure we only set it once
    if (libraryLoaded.get()) {
      return;
    }
    if (libraryLoaded.compareAndSet(false, true)) {
      // if you see a library failed to load error, you need to run mvn test-compile at least once first
      SQLite.setLibraryPath("target/lib");
    }
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
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {

    startServer();

    initializeClient();

    createTable();
  }

  private void createTable() {
    KeySchemaElement[] keySchemaElements;
    if (rangeKeyName == null) {
      keySchemaElements = new KeySchemaElement[] {
          KeySchemaElement.builder().attributeName(hashKeyName).keyType(KeyType.HASH).build(),
      };
    } else {
      keySchemaElements = new KeySchemaElement[] {
          KeySchemaElement.builder().attributeName(hashKeyName).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(rangeKeyName).keyType(KeyType.RANGE).build(),
      };
    }

    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName(tableName)
        .keySchema(keySchemaElements)
        .attributeDefinitions(attributeDefinitions.isEmpty() ? null : attributeDefinitions)
        .globalSecondaryIndexes(globalSecondaryIndexes.isEmpty() ? null : globalSecondaryIndexes)
        .localSecondaryIndexes(localSecondaryIndexes.isEmpty() ? null : localSecondaryIndexes)
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(readCapacityUnits)
            .writeCapacityUnits(writeCapacityUnits)
            .build())
        .build();

    getDynamoDbClient().createTable(createTableRequest);
  }

  private void startServer() throws Exception {
    // Even though we're using AWS SDK v2, Dynamo's local implementation's canonical location
    // is within v1 (https://github.com/aws/aws-sdk-java-v2/issues/982).  This does support
    // v2 clients, though.
    loadLibrary();
    ServerSocket serverSocket = new ServerSocket(0);
    serverSocket.setReuseAddress(false);
    port = serverSocket.getLocalPort();
    serverSocket.close();
    server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", String.valueOf(port)});
    server.start();
  }

  private void initializeClient() {
    dynamoDB2 = DynamoDbClient.builder()
        .endpointOverride(URI.create("http://localhost:" + port))
        .region(Region.of("local-test-region"))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("accessKey", "secretKey")))
        .build();
    dynamoAsyncDB2 = DynamoDbAsyncClient.builder()
        .endpointOverride(URI.create("http://localhost:" + port))
        .region(Region.of("local-test-region"))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("accessKey", "secretKey")))
        .build();
    legacyDynamoClient = AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, "local-test-region"))
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey")))
        .build();
  }

  public static class DynamoDbExtensionBuilder {

    private String tableName = DEFAULT_TABLE_NAME;

    private String hashKey;
    private String rangeKey;

    private final List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    private final List<GlobalSecondaryIndex> globalSecondaryIndexes = new ArrayList<>();
    private final List<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<>();

    private final long readCapacityUnits = DEFAULT_PROVISIONED_THROUGHPUT.readCapacityUnits();
    private final long writeCapacityUnits = DEFAULT_PROVISIONED_THROUGHPUT.writeCapacityUnits();

    private DynamoDbExtensionBuilder() {

    }

    public DynamoDbExtensionBuilder tableName(String databaseName) {
      this.tableName = databaseName;
      return this;
    }

    public DynamoDbExtensionBuilder hashKey(String hashKey) {
      this.hashKey = hashKey;
      return this;
    }

    public DynamoDbExtensionBuilder rangeKey(String rangeKey) {
      this.rangeKey = rangeKey;
      return this;
    }

    public DynamoDbExtensionBuilder attributeDefinition(AttributeDefinition attributeDefinition) {
      attributeDefinitions.add(attributeDefinition);
      return this;
    }

    public DynamoDbExtensionBuilder globalSecondaryIndex(GlobalSecondaryIndex index) {
      globalSecondaryIndexes.add(index);
      return this;
    }

    public DynamoDbExtensionBuilder localSecondaryIndex(LocalSecondaryIndex index) {
      localSecondaryIndexes.add(index);
      return this;
    }

    public DynamoDbExtension build() {
      return new DynamoDbExtension(tableName, hashKey, rangeKey,
          attributeDefinitions, globalSecondaryIndexes, localSecondaryIndexes, readCapacityUnits, writeCapacityUnits);
    }
  }

  public DynamoDbClient getDynamoDbClient() {
    return dynamoDB2;
  }

  public DynamoDbAsyncClient getDynamoDbAsyncClient() {
    return dynamoAsyncDB2;
  }

  public AmazonDynamoDB getLegacyDynamoClient() {
    return legacyDynamoClient;
  }

  public String getTableName() {
    return tableName;
  }
}
