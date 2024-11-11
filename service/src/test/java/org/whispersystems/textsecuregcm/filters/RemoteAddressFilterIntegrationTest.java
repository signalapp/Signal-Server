/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.servlet.DispatcherType;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.eclipse.jetty.util.HostPort;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

@ExtendWith(DropwizardExtensionsSupport.class)
class RemoteAddressFilterIntegrationTest {

  private static final String WEBSOCKET_PREFIX = "/websocket";
  private static final String REMOTE_ADDRESS_PATH = "/remoteAddress";
  private static final String WS_REQUEST_PATH = "/wsRequest";

  // The Grizzly test container does not match the Jetty container used in real deployments, and JettyTestContainerFactory
  // in jersey-test-framework-provider-jetty doesn’t easily support @Context HttpServletRequest, so this test runs a
  // full Jetty server in a separate process
  private static final DropwizardAppExtension<Configuration> EXTENSION = new DropwizardAppExtension<>(
      TestApplication.class);

  @Nested
  class Rest {

    @ParameterizedTest
    @ValueSource(strings = {"127.0.0.1", "0:0:0:0:0:0:0:1"})
    void testRemoteAddress(String ip) throws Exception {
      final Set<String> addresses = Arrays.stream(InetAddress.getAllByName("localhost"))
          .map(InetAddress::getHostAddress)
          .collect(Collectors.toSet());

      assumeTrue(addresses.contains(ip), String.format("localhost does not resolve to %s", ip));

      Client client = EXTENSION.client();

      final RemoteAddressFilterIntegrationTest.TestResponse response = client.target(
              String.format("http://%s:%d%s", HostPort.normalizeHost(ip), EXTENSION.getLocalPort(), REMOTE_ADDRESS_PATH))
          .request("application/json")
          .get(RemoteAddressFilterIntegrationTest.TestResponse.class);

      assertEquals(ip, response.remoteAddress());
    }
  }

  @Nested
  class WebSocket {

    private WebSocketClient client;

    @BeforeEach
    void setUp() throws Exception {
      client = new WebSocketClient();
      client.start();
    }

    @AfterEach
    void tearDown() throws Exception {
      client.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"127.0.0.1", "0:0:0:0:0:0:0:1"})
    void testRemoteAddress(String ip) throws Exception {
      final Set<String> addresses = Arrays.stream(InetAddress.getAllByName("localhost"))
          .map(InetAddress::getHostAddress)
          .collect(Collectors.toSet());

      assumeTrue(addresses.contains(ip), String.format("localhost does not resolve to %s", ip));

      final CompletableFuture<byte[]> responseFuture = new CompletableFuture<>();
      final ClientEndpoint clientEndpoint = new ClientEndpoint(WS_REQUEST_PATH, responseFuture);

      client.connect(clientEndpoint,
          URI.create(
              String.format("ws://%s:%d%s", HostPort.normalizeHost(ip), EXTENSION.getLocalPort(),
                  WEBSOCKET_PREFIX + REMOTE_ADDRESS_PATH)));

      final byte[] responseBytes = responseFuture.get(1, TimeUnit.SECONDS);

      final TestResponse response = SystemMapper.jsonMapper().readValue(responseBytes, TestResponse.class);

      assertEquals(ip, response.remoteAddress());
    }
  }

  private static class ClientEndpoint implements WebSocketListener {

    private final String requestPath;
    private final CompletableFuture<byte[]> responseFuture;
    private final WebSocketMessageFactory messageFactory;

    ClientEndpoint(String requestPath, CompletableFuture<byte[]> responseFuture) {

      this.requestPath = requestPath;
      this.responseFuture = responseFuture;
      this.messageFactory = new ProtobufWebSocketMessageFactory();
    }

    @Override
    public void onWebSocketConnect(final Session session) {
      final byte[] requestBytes = messageFactory.createRequest(Optional.of(1L), "GET", requestPath,
          List.of("Accept: application/json"),
          Optional.empty()).toByteArray();
      try {
        session.getRemote().sendBytes(ByteBuffer.wrap(requestBytes));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onWebSocketBinary(final byte[] payload, final int offset, final int length) {

      try {
        WebSocketMessage webSocketMessage = messageFactory.parseMessage(payload, offset, length);

        if (Objects.requireNonNull(webSocketMessage.getType()) == WebSocketMessage.Type.RESPONSE_MESSAGE) {
          assert 200 == webSocketMessage.getResponseMessage().getStatus();
          responseFuture.complete(webSocketMessage.getResponseMessage().getBody().orElseThrow());
        } else {
          throw new RuntimeException("Unexpected message type: " + webSocketMessage.getType());
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }

    }

  }

  public static abstract class TestController {

    @GET
    public RemoteAddressFilterIntegrationTest.TestResponse get(@Context ContainerRequestContext context) {

      return new RemoteAddressFilterIntegrationTest.TestResponse(
          (String) context.getProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME));
    }
  }

  @Path(REMOTE_ADDRESS_PATH)
  public static class TestRemoteAddressController extends TestController {

  }

  @Path(WS_REQUEST_PATH)
  public static class TestWebSocketController extends TestController {

  }

  public record TestResponse(String remoteAddress) {

  }

  public static class TestApplication extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration,
        final Environment environment) throws Exception {

      environment.servlets().addFilter("RemoteAddressFilterRemoteAddress", new RemoteAddressFilter())
          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, REMOTE_ADDRESS_PATH,
              WEBSOCKET_PREFIX + REMOTE_ADDRESS_PATH);

      environment.jersey().register(new TestRemoteAddressController());

      // WebSocket set up
      final WebSocketConfiguration webSocketConfiguration = new WebSocketConfiguration();

      WebSocketEnvironment<TestPrincipal> webSocketEnvironment = new WebSocketEnvironment<>(environment,
          webSocketConfiguration, Duration.ofMillis(1000));

      webSocketEnvironment.jersey().register(new TestWebSocketController());

      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), null);

      WebSocketResourceProviderFactory<TestPrincipal> webSocketServlet = new WebSocketResourceProviderFactory<>(
          webSocketEnvironment, TestPrincipal.class, webSocketConfiguration,
          RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);

      environment.servlets().addServlet("WebSocketRemoteAddress", webSocketServlet)
          .addMapping(WEBSOCKET_PREFIX + REMOTE_ADDRESS_PATH);

    }
  }

  /**
   * A minimal {@code Principal} implementation, only used to satisfy constructors
   */
  public static class TestPrincipal implements Principal {

    // Principal implementation

    @Override
    public String getName() {
      return null;
    }

    @Override
    public boolean implies(final Subject subject) {
      return false;
    }
  }
}
