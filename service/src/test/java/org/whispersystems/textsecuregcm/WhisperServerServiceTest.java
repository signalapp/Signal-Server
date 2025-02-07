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
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.metrics.NoopAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
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
  }

  private static final WebSocketClient webSocketClient = new WebSocketClient();

  private static final DropwizardAppExtension<WhisperServerConfiguration> EXTENSION = new DropwizardAppExtension<>(
      WhisperServerService.class, Resources.getResource("config/test.yml").getPath());


  @AfterAll
  static void teardown() {
    System.clearProperty("secrets.bundle.filename");
  }

  @BeforeAll
  static void setUp() throws Exception {
    webSocketClient.start();
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
    final long start = System.currentTimeMillis();

    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();

    EXTENSION.getTestSupport().getEnvironment().getApplicationContext().getServer()
        .addEventListener(new LifeCycle.Listener() {
          @Override
          public void lifeCycleStopped(final LifeCycle event) {
            // closed by org.eclipse.jetty.websocket.common.SessionTracker during the container Lifecycle stopping phase
            assertEquals(StatusCode.SHUTDOWN, testWebsocketListener.closeFuture().getNow(-1));
          }
        });

    // Session is Closeable, but we intentionally keep it open so that we can confirm the container Lifecycle behavior
    final Session session = webSocketClient.connect(testWebsocketListener,
            URI.create(String.format("ws://localhost:%d/v1/websocket/", EXTENSION.getLocalPort())))
        .join();
    final long sessionTimestamp = Long.parseLong(session.getUpgradeResponse().getHeader(HeaderUtils.TIMESTAMP_HEADER));
    assertTrue(sessionTimestamp >= start);

    final WebSocketResponseMessage keepAlive = testWebsocketListener.doGet("/v1/keepalive").join();
    assertEquals(200, keepAlive.getStatus());
    final long keepAliveTimestamp = Long.parseLong(
        keepAlive.getHeaders().get(HeaderUtils.TIMESTAMP_HEADER.toLowerCase()));
    assertTrue(keepAliveTimestamp >= start);

    final WebSocketResponseMessage whoami = testWebsocketListener.doGet("/v1/accounts/whoami").join();
    assertEquals(401, whoami.getStatus());
    final long whoamiTimestamp = Long.parseLong(whoami.getHeaders().get(HeaderUtils.TIMESTAMP_HEADER.toLowerCase()));
    assertTrue(whoamiTimestamp >= start);


  }

  @Test
  void rest() throws Exception {
    // test unauthenticated rest
    final long start = System.currentTimeMillis();

    final Response whoami = EXTENSION.client().target(
        "http://localhost:%d/v1/accounts/whoami".formatted(EXTENSION.getLocalPort())).request().get();

    assertEquals(401, whoami.getStatus());
    final List<Object> timestampValues = whoami.getHeaders().get(HeaderUtils.TIMESTAMP_HEADER.toLowerCase());
    assertEquals(1, timestampValues.size());

    final long whoamiTimestamp = Long.parseLong(timestampValues.getFirst().toString());
    assertTrue(whoamiTimestamp >= start);
  }

  @Test
  void dynamoDb() {
    // confirm that local dynamodb nominally works

    final AwsCredentialsProvider awsCredentialsProvider = EXTENSION.getConfiguration().getAwsCredentialsConfiguration()
        .build();

    final DynamoDbClient dynamoDbClient = EXTENSION.getConfiguration().getDynamoDbClientConfiguration()
        .buildSyncClient(awsCredentialsProvider, new NoopAwsSdkMetricPublisher());

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
