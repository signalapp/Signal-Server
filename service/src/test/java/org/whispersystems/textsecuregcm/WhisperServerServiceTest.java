/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.util.Resources;
import java.net.URI;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

@ExtendWith(DropwizardExtensionsSupport.class)
class WhisperServerServiceTest {

  static {
    System.setProperty("secrets.bundle.filename",
        Resources.getResource("config/test-secrets-bundle.yml").getPath());
    // needed for AppConfigDataClient initialization
    System.setProperty("aws.region", "local-test-region");
  }

  private static final DropwizardAppExtension<WhisperServerConfiguration> EXTENSION = new DropwizardAppExtension<>(
      WhisperServerService.class, Resources.getResource("config/test.yml").getPath());

  private WebSocketClient webSocketClient;

  @AfterAll
  static void teardown() {
    System.clearProperty("secrets.bundle.filename");
    System.clearProperty("aws.region");
  }

  @BeforeEach
  void setUp() throws Exception {
    webSocketClient = new WebSocketClient();
    webSocketClient.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    webSocketClient.stop();
  }

  @Test
  void start() throws Exception {
    // make sure the service nominally starts and responds to health checks

    Client client = EXTENSION.client();

    final Response ping = client.target(
            String.format("http://localhost:%d%s", EXTENSION.getAdminPort(), "/ping"))
        .request("application/json")
        .get();

    assertEquals(200, ping.getStatus());

    final Response healthCheck = client.target(
            String.format("http://localhost:%d%s", EXTENSION.getLocalPort(), "/health-check"))
        .request("application/json")
        .get();

    assertEquals(200, healthCheck.getStatus());
  }

  @Test
  void websocket() throws Exception {
    // test unauthenticated websocket

    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();
    webSocketClient.connect(testWebsocketListener,
            URI.create(String.format("ws://localhost:%d/v1/websocket/", EXTENSION.getLocalPort())))
        .join();

    final WebSocketResponseMessage keepAlive = testWebsocketListener.doGet("/v1/keepalive").join();

    assertEquals(200, keepAlive.getStatus());
  }

  @Test
  void dynamoDb() {
    // confirm that local dynamodb nominally works

    final AwsCredentialsProvider awsCredentialsProvider = EXTENSION.getConfiguration().getAwsCredentialsConfiguration()
        .build();

    try (DynamoDbClient dynamoDbClient = EXTENSION.getConfiguration().getDynamoDbClientConfiguration()
        .buildSyncClient(awsCredentialsProvider)) {

      final DynamoDbExtension.TableSchema numbers = DynamoDbExtensionSchema.Tables.NUMBERS;
      final AttributeValue numberAV = AttributeValues.s("+12125550001");

      final GetItemResponse notFoundResponse = dynamoDbClient.getItem(GetItemRequest.builder()
          .tableName(numbers.tableName())
          .key(Map.of(numbers.hashKeyName(), numberAV))
          .build());

      assertFalse(notFoundResponse.hasItem());

      dynamoDbClient.putItem(PutItemRequest.builder()
          .tableName(numbers.tableName())
          .item(Map.of(numbers.hashKeyName(), numberAV))
          .build());

      final GetItemResponse foundResponse = dynamoDbClient.getItem(GetItemRequest.builder()
          .tableName(numbers.tableName())
          .key(Map.of(numbers.hashKeyName(), numberAV))
          .build());

      assertTrue(foundResponse.hasItem());

      dynamoDbClient.deleteItem(DeleteItemRequest.builder()
          .tableName(numbers.tableName())
          .key(Map.of(numbers.hashKeyName(), numberAV))
          .build());
    }

  }

}
