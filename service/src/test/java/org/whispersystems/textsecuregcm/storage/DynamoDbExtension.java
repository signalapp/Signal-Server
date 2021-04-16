package org.whispersystems.textsecuregcm.storage;

import com.almworks.sqlite4java.SQLite;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class DynamoDbExtension implements BeforeEachCallback, AfterEachCallback {

  static final  String DEFAULT_TABLE_NAME = "test_table";

  static final ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT = new ProvisionedThroughput(20L, 20L);

  private DynamoDBProxyServer server;
  private int port;

  private final String tableName;
  private final String hashKeyName;
  private final String rangeKeyName;

  private final List<AttributeDefinition> attributeDefinitions;
  private final List<GlobalSecondaryIndex> globalSecondaryIndexes;

  private final long readCapacityUnits;
  private final long writeCapacityUnits;

  private AmazonDynamoDB client;
  private DynamoDB dynamoDB;

  private DynamoDbExtension(String tableName, String hashKey, String rangeKey, List<AttributeDefinition> attributeDefinitions, List<GlobalSecondaryIndex> globalSecondaryIndexes, long readCapacityUnits,
      long writeCapacityUnits) {

    this.tableName = tableName;
    this.hashKeyName = hashKey;
    this.rangeKeyName = rangeKey;

    this.readCapacityUnits = readCapacityUnits;
    this.writeCapacityUnits = writeCapacityUnits;

    this.attributeDefinitions = attributeDefinitions;
    this.globalSecondaryIndexes = globalSecondaryIndexes;
  }

  public static DynamoDbExtensionBuilder builder() {
    return new DynamoDbExtensionBuilder();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
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
          new KeySchemaElement(hashKeyName, "HASH"),
      };
    } else {
      keySchemaElements = new KeySchemaElement[] {
          new KeySchemaElement(hashKeyName, "HASH"),
          new KeySchemaElement(rangeKeyName, "RANGE")
      };
    }

    final CreateTableRequest createTableRequest = new CreateTableRequest()
        .withTableName(tableName)
        .withKeySchema(keySchemaElements)
        .withAttributeDefinitions(attributeDefinitions.isEmpty() ? null : attributeDefinitions)
        .withGlobalSecondaryIndexes(globalSecondaryIndexes.isEmpty() ? null : globalSecondaryIndexes)
        .withProvisionedThroughput(new ProvisionedThroughput(readCapacityUnits, writeCapacityUnits));

    getDynamoDB().createTable(createTableRequest);
  }

  private void startServer() throws Exception {
    SQLite.setLibraryPath("target/lib");  // if you see a library failed to load error, you need to run mvn test-compile at least once first
    ServerSocket serverSocket = new ServerSocket(0);
    serverSocket.setReuseAddress(false);
    port = serverSocket.getLocalPort();
    serverSocket.close();
    server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", String.valueOf(port)});
    server.start();
  }

  private void initializeClient() {
    client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, "local-test-region"))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey")))
            .build();

    dynamoDB = new DynamoDB(client);
  }

  static class DynamoDbExtensionBuilder {
    private String tableName = DEFAULT_TABLE_NAME;

    private String hashKey;
    private String rangeKey;

    private List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    private List<GlobalSecondaryIndex> globalSecondaryIndexes = new ArrayList<>();

    private long readCapacityUnits = DEFAULT_PROVISIONED_THROUGHPUT.getReadCapacityUnits();
    private long writeCapacityUnits = DEFAULT_PROVISIONED_THROUGHPUT.getWriteCapacityUnits();

    private DynamoDbExtensionBuilder() {

    }

    DynamoDbExtensionBuilder tableName(String databaseName) {
      this.tableName = databaseName;
      return this;
    }

    DynamoDbExtensionBuilder hashKey(String hashKey) {
      this.hashKey = hashKey;
      return this;
    }

    DynamoDbExtensionBuilder rangeKey(String rangeKey) {
      this.rangeKey = rangeKey;
      return this;
    }

    DynamoDbExtensionBuilder attributeDefinition(AttributeDefinition attributeDefinition) {
      attributeDefinitions.add(attributeDefinition);
      return this;
    }

    public DynamoDbExtensionBuilder globalSecondaryIndex(GlobalSecondaryIndex index) {
      globalSecondaryIndexes.add(index);
      return this;
    }

    DynamoDbExtension build() {
      return new DynamoDbExtension(tableName, hashKey, rangeKey,
          attributeDefinitions, globalSecondaryIndexes, readCapacityUnits, writeCapacityUnits);
    }
  }

  public AmazonDynamoDB getClient() {
    return client;
  }

  public DynamoDB getDynamoDB() {
    return dynamoDB;
  }

  public String getTableName() {
    return tableName;
  }
}
