/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.util.Resources;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.transport.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.chat.account.AccountsAnonymousGrpc;
import org.signal.chat.account.CheckAccountExistenceRequest;
import org.signal.chat.account.CheckAccountExistenceResponse;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.NoopAwsSdkMetricPublisher;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
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
  private static final int OMNIBUS_PORT = findAvailablePort();

  private static WebSocketClient webSocketClient;
  private static WebSocketClient h2WebSocketClient;

  private static final DropwizardAppExtension<WhisperServerConfiguration> EXTENSION = new DropwizardAppExtension<>(
      WhisperServerService.class, Resources.getResource("config/test.yml").getPath(),
      // Tables will be created by the local DynamoDbExtension
      ConfigOverride.config("dynamoDbClient.initTables", "false"),
      ConfigOverride.config("grpc.port", String.valueOf(OMNIBUS_PORT)));

  @RegisterExtension
  public static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.values());

  @AfterAll
  static void teardown() {
    System.clearProperty("secrets.bundle.filename");
  }

  @BeforeAll
  static void setUp() throws Exception {
    final ClientConnector clientConnector = new ClientConnector();
    final HTTP2Client http2Client = new HTTP2Client(clientConnector);
    final HttpClient httpClient = new HttpClient(new HttpClientTransportOverHTTP2(http2Client));
    h2WebSocketClient = new WebSocketClient(httpClient);
    webSocketClient = new WebSocketClient();
    webSocketClient.start();
    h2WebSocketClient.start();
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void websocket(final boolean useH2) throws Exception {
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
    final Session session = (useH2 ? h2WebSocketClient : webSocketClient).connect(testWebsocketListener,
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

    final DynamoDbClient dynamoDbClient = getDynamoDbClient();

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


  @Test
  void omnibusWebsocket() throws Exception {
    final HTTP2Client http2Client = new HTTP2Client(new ClientConnector());
    final WebSocketClient h2WebSocketClient =
        new WebSocketClient(new HttpClient(new HttpClientTransportOverHTTP2(http2Client)));
    h2WebSocketClient.start();

    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();
    final Session session = h2WebSocketClient.connect(testWebsocketListener,
            URI.create(String.format("ws://localhost:%d/v1/websocket/", OMNIBUS_PORT)))
        .join();
    final WebSocketResponseMessage keepAlive = testWebsocketListener.doGet("/v1/keepalive").join();
    assertEquals(200, keepAlive.getStatus());
    final WebSocketResponseMessage whoami = testWebsocketListener.doGet("/v1/accounts/whoami").join();
    assertEquals(401, whoami.getStatus());
    session.close();
    h2WebSocketClient.stop();
  }

  @Test
  void omnibusGrpc() throws Exception {
    final ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", OMNIBUS_PORT)
        .usePlaintext()
        .build();

    final Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of("X-Forwarded-For", Metadata.ASCII_STRING_MARSHALLER), "127.0.0.1");

    final AccountsAnonymousGrpc.AccountsAnonymousBlockingStub stub = AccountsAnonymousGrpc
        .newBlockingStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    final CheckAccountExistenceResponse response = stub.checkAccountExistence(
        CheckAccountExistenceRequest.newBuilder()
            .setServiceIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(UUIDUtil.toByteString(UUID.randomUUID()))
                .build())
            .build());

    assertFalse(response.getAccountExists());
    channel.shutdownNow();
    channel.awaitTermination(1, TimeUnit.SECONDS);
  }

  private static DynamoDbClient getDynamoDbClient() {
    final AwsCredentialsProvider awsCredentialsProvider = EXTENSION.getConfiguration().getAwsCredentialsConfiguration()
        .build();

    return EXTENSION.getConfiguration().getDynamoDbClientConfiguration()
        .buildSyncClient(awsCredentialsProvider, new NoopAwsSdkMetricPublisher());
  }

  private static int findAvailablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}
