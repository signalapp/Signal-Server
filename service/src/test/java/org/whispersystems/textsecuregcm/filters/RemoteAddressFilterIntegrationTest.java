/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.common.net.HttpHeaders;
import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.util.HostPort;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.jetty.JettyHttpConfigurationCustomizer;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.messages.WebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

@ExtendWith(DropwizardExtensionsSupport.class)
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RemoteAddressFilterIntegrationTest {

  private static final String WEBSOCKET_PREFIX = "/websocket";
  private static final String REMOTE_ADDRESS_PATH = "/remoteAddress";
  private static final String WS_REQUEST_PATH = "/wsRequest";

  // The Grizzly test container does not match the Jetty container used in real deployments, and JettyTestContainerFactory
  // in jersey-test-framework-provider-jetty doesn’t easily support @Context HttpServletRequest, so this test runs a
  // full Jetty server in a separate process
  private static final DropwizardAppExtension<Configuration> EXTENSION = new DropwizardAppExtension<>(
      TestApplication.class, null,
      ConfigOverride.config("server.applicationConnectors[0].type", "h2c"),
      ConfigOverride.config("server.applicationConnectors[0].useForwardedHeaders", "true"));

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

    @ParameterizedTest
    @ValueSource(strings = {"127.0.0.3", "0:0:0:0:0:0:dead:beef"})
    void testForwardedForHeader(final String ip) throws Exception {
      final Client client = EXTENSION.client();

      final RemoteAddressFilterIntegrationTest.TestResponse response = client.target(
              String.format("http://%s:%d%s", "localhost", EXTENSION.getLocalPort(), REMOTE_ADDRESS_PATH))
          .request("application/json")
          .header(HttpHeaders.X_FORWARDED_FOR, ip)
          .get(RemoteAddressFilterIntegrationTest.TestResponse.class);

      assertEquals(ip, response.remoteAddress());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Forwarded", "X-Forwarded-Host", "X-Forwarded-Server", "X-Forwarded-Proto"})
    void testOtherHeadersIgnored(final String header) throws Exception {
      final Client client = EXTENSION.client();

      final String ip = "127.0.0.3";

      final RemoteAddressFilterIntegrationTest.TestResponse response = client.target(
              String.format("http://%s:%d%s", "localhost", EXTENSION.getLocalPort(), REMOTE_ADDRESS_PATH))
          .request("application/json")
          .header(header, ip)
          .get(RemoteAddressFilterIntegrationTest.TestResponse.class);

      assertNotEquals(ip, response.remoteAddress(), "header " + header + " should be ignored");
      assertEquals("127.0.0.1", response.remoteAddress());
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


    @ParameterizedTest
    @ValueSource(strings = {"127.0.0.3", "0:0:0:0:0:0:dead:beef"})
    void testForwardedForHeader(final String ip) throws Exception {
      final CompletableFuture<byte[]> responseFuture = new CompletableFuture<>();
      final ClientEndpoint clientEndpoint = new ClientEndpoint(WS_REQUEST_PATH, responseFuture);
      final ClientUpgradeRequest upgradeRequest = new ClientUpgradeRequest(URI.create(String.format("ws://%s:%d%s",
          "localhost",
          EXTENSION.getLocalPort(),
          WEBSOCKET_PREFIX + REMOTE_ADDRESS_PATH)));
      upgradeRequest.setHeader(HttpHeaders.X_FORWARDED_FOR, ip);
      client.connect(clientEndpoint, upgradeRequest);

      final byte[] responseBytes = responseFuture.get(1, TimeUnit.SECONDS);
      final TestResponse response = SystemMapper.jsonMapper().readValue(responseBytes, TestResponse.class);
      assertEquals(ip, response.remoteAddress());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Forwarded", "X-Forwarded-Host", "X-Forwarded-Server", "X-Forwarded-Proto"})
    void testOtherHeadersIgnored(final String header) throws Exception {
      final String ip = "127.0.0.3";

      final CompletableFuture<byte[]> responseFuture = new CompletableFuture<>();
      final ClientEndpoint clientEndpoint = new ClientEndpoint(WS_REQUEST_PATH, responseFuture);
      final ClientUpgradeRequest upgradeRequest = new ClientUpgradeRequest(URI.create(String.format("ws://%s:%d%s",
          "localhost",
          EXTENSION.getLocalPort(),
          WEBSOCKET_PREFIX + REMOTE_ADDRESS_PATH)));
      upgradeRequest.setHeader(header, ip);
      client.connect(clientEndpoint, upgradeRequest);

      final byte[] responseBytes = responseFuture.get(1, TimeUnit.SECONDS);
      final TestResponse response = SystemMapper.jsonMapper().readValue(responseBytes, TestResponse.class);

      assertNotEquals(ip, response.remoteAddress(), "header " + header + " should be ignored");
      assertEquals("127.0.0.1", response.remoteAddress());
    }
  }

  public static class ClientEndpoint implements Session.Listener.AutoDemanding {

    private final String requestPath;
    private final CompletableFuture<byte[]> responseFuture;
    private final WebSocketMessageFactory messageFactory;

    ClientEndpoint(String requestPath, CompletableFuture<byte[]> responseFuture) {

      this.requestPath = requestPath;
      this.responseFuture = responseFuture;
      this.messageFactory = new ProtobufWebSocketMessageFactory();
    }

    @Override
    public void onWebSocketOpen(final Session session) {
      final byte[] requestBytes = messageFactory.createRequest(Optional.of(1L), "GET", requestPath,
          List.of("Accept: application/json"),
          Optional.empty()).toByteArray();

      session.sendBinary(ByteBuffer.wrap(requestBytes), Callback.NOOP);
    }

    @Override
    public void onWebSocketBinary(final ByteBuffer payload, final Callback callback) {

      try {
        WebSocketMessage webSocketMessage = messageFactory.parseMessage(payload);

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

      environment.jersey().register(new TestRemoteAddressController());

      // WebSocket set up
      final WebSocketConfiguration webSocketConfiguration = new WebSocketConfiguration();

      WebSocketEnvironment<TestPrincipal> webSocketEnvironment = new WebSocketEnvironment<>(environment,
          webSocketConfiguration, Duration.ofMillis(1000));

      webSocketEnvironment.jersey().register(new TestWebSocketController());

      WebSocketResourceProviderFactory<TestPrincipal> webSocketServlet = new WebSocketResourceProviderFactory<>(
          webSocketEnvironment, TestPrincipal.class,
          RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);

      environment.lifecycle().addEventListener(new JettyHttpConfigurationCustomizer());

      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), (servletContext, container) -> {
        container.addMapping(WEBSOCKET_PREFIX + REMOTE_ADDRESS_PATH, webSocketServlet);
        PriorityFilter.ensureFilter(servletContext, new RemoteAddressFilter());
      });
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
