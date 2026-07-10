package org.whispersystems.textsecuregcm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.whispersystems.textsecuregcm.filters.RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.server.DefaultServerFactory;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.http2.Http2CConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.testing.DropwizardTestSupport;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.transport.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.component.Graceful;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.filters.PriorityFilter;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class GracefulShutdownIntegrationTest {
  private WebSocketClient webSocketClient;
  private WebSocketClient h2WebSocketClient;

  @BeforeEach
  void setUp() throws Exception {
    webSocketClient = new WebSocketClient();
    webSocketClient.start();

    final ClientConnector clientConnector = new ClientConnector();
    final HTTP2Client http2Client = new HTTP2Client(clientConnector);
    h2WebSocketClient = new WebSocketClient(new HttpClient(new HttpClientTransportOverHTTP2(http2Client)));
    h2WebSocketClient.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    webSocketClient.stop();
    h2WebSocketClient.stop();
  }

  private DropwizardTestSupport<Configuration> configureApplication() {
    final Http2CConnectorFactory applicationConnector = new Http2CConnectorFactory();
    applicationConnector.setPort(0);
    final HttpConnectorFactory adminConnector = new HttpConnectorFactory();
    adminConnector.setPort(0);

    final DefaultServerFactory serverFactory = new DefaultServerFactory();
    serverFactory.setApplicationConnectors(List.of(applicationConnector));
    serverFactory.setAdminConnectors(List.of(adminConnector));

    final Configuration configuration = new Configuration();
    configuration.setServerFactory(serverFactory);

    return new DropwizardTestSupport<>(TestApplication.class, configuration);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void jettyClosesOpenWebsocketSessions(final boolean useH2) throws Exception {
    final DropwizardTestSupport<Configuration> support = configureApplication();
    support.before();
    try {
      final Server server = support.getEnvironment().getApplicationContext().getServer();

      final TestWebsocketListener listener = new TestWebsocketListener();
      final Session session = (useH2 ? h2WebSocketClient : webSocketClient)
          .connect(listener, URI.create("ws://localhost:%d/websocket".formatted(support.getLocalPort())))
          .join();
      assertTrue(session.isOpen(), "session should be open before shutdown");

      // Open websockets should be closed by org.eclipse.jetty.websocket.common.SessionTracker
      // during the container Lifecycle stopping phase
      final CompletableFuture<Integer> closeObserved = listener.closeFuture();

      // Keep the jetty graceful-shutdown window open until the client has observed the close
      server.addBean(new Graceful() {
        @Override
        public CompletableFuture<Void> shutdown() {
          return closeObserved.thenApply(_ -> null);
        }

        @Override
        public boolean isShutdown() {
          return closeObserved.isDone();
        }
      });
      server.setStopTimeout(TimeUnit.SECONDS.toMillis(10));
      server.stop();

      final int shutdownCode =  closeObserved.get(1, TimeUnit.SECONDS);

      // In h2, when jetty shuts down a websocket with abnormal status code (e.g. 1001) it also cancels the underlying
      // H2 stream. It's possible that the stream cancellation makes it to the wire before the close frame which will
      // be parsed as a 1006, so we accept both . In practice, a 1001 vs an RST_STREAM shouldn't make a difference to a
      // client. We just care that jetty is attempting to shut down all websockets during graceful shutdowns.
      assertTrue(shutdownCode == StatusCode.SHUTDOWN || (shutdownCode == StatusCode.NO_CODE && useH2), "Shutdown code should be 1001 or 1006, was " + shutdownCode);
    } finally {
      support.after();
    }
  }

  public static class TestApplication extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration, final Environment environment) throws Exception {
      final WebSocketEnvironment<AuthenticatedDevice> webSocketEnvironment =
          new WebSocketEnvironment<>(environment, new WebSocketConfiguration());
      webSocketEnvironment.jersey().register(new RemoteAddressFilter());
      webSocketEnvironment.setAuthenticator(upgradeRequest -> Optional.of(mock(AuthenticatedDevice.class)));
      webSocketEnvironment.setConnectListener(context -> {});

      final WebSocketResourceProviderFactory<AuthenticatedDevice> webSocketServlet =
          new WebSocketResourceProviderFactory<>(webSocketEnvironment, AuthenticatedDevice.class,
              REMOTE_ADDRESS_ATTRIBUTE_NAME);

      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(),
          (servletContext, container) -> {
            container.addMapping("/websocket", webSocketServlet);
            PriorityFilter.ensureFilter(servletContext, new RemoteAddressFilter());
          });
    }
  }
}
