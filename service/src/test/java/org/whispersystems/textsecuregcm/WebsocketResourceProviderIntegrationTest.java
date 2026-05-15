package org.whispersystems.textsecuregcm;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.whispersystems.textsecuregcm.filters.RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletRegistration;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.glassfish.jersey.server.ManagedAsync;
import org.glassfish.jersey.server.ServerProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

@ExtendWith(DropwizardExtensionsSupport.class)
public class WebsocketResourceProviderIntegrationTest {
  private static final DropwizardAppExtension<Configuration> DROPWIZARD_APP_EXTENSION =
      new DropwizardAppExtension<>(TestApplication.class);


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


  public static class TestApplication extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration, final Environment environment) throws Exception {
      final TestController testController = new TestController();

      final WebSocketConfiguration webSocketConfiguration = new WebSocketConfiguration();

      final WebSocketEnvironment<AuthenticatedDevice> webSocketEnvironment =
          new WebSocketEnvironment<>(environment, webSocketConfiguration);

      environment.jersey().register(testController);
      environment.servlets()
          .addFilter("RemoteAddressFilter", new RemoteAddressFilter())
          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");
      webSocketEnvironment.jersey().register(testController);
      webSocketEnvironment.jersey().register(new RemoteAddressFilter());
      webSocketEnvironment.setAuthenticator(upgradeRequest -> Optional.of(mock(AuthenticatedDevice.class)));

      webSocketEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
      webSocketEnvironment.setConnectListener(webSocketSessionContext -> {
      });

      final WebSocketResourceProviderFactory<AuthenticatedDevice> webSocketServlet =
          new WebSocketResourceProviderFactory<>(webSocketEnvironment, AuthenticatedDevice.class,
              webSocketConfiguration, REMOTE_ADDRESS_ATTRIBUTE_NAME);

      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), null);

      final ServletRegistration.Dynamic websocketServlet =
          environment.servlets().addServlet("WebSocket", webSocketServlet);

      websocketServlet.addMapping("/websocket");
      websocketServlet.setAsyncSupported(true);
    }
  }


  @ParameterizedTest
  // Jersey's content-length buffering by default does not buffer responses with a content-length of > 8192. We disable
  // that buffering and do our own though, so the 9000 byte case should work.
  @ValueSource(ints = {0, 1, 100, 1025, 9000})
  public void contentLength(int length) throws IOException {
    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();
    client.connect(testWebsocketListener,
        URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())));

    final WebSocketResponseMessage readResponse = testWebsocketListener.doGet("/test/%d".formatted(length)).join();
    assertThat(readResponse.getHeaders().get(HttpHeaders.CONTENT_LENGTH.toLowerCase()))
        .isEqualTo(Integer.toString(length));
  }


  @Path("/test")
  public static class TestController {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{size}")
    @ManagedAsync
    public String get(@PathParam("size") int size) {
      return RandomStringUtils.secure().nextAscii(size);
    }

    @PUT
    @ManagedAsync
    public String put() {
      return "put";
    }
  }
}
