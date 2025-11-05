package org.whispersystems.textsecuregcm;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.filters.RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME;

import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletRegistration;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.textsecuregcm.websocket.ProvisioningConnectListener;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.WebsocketHeaders;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.messages.InvalidMessageException;
import org.whispersystems.websocket.messages.WebSocketMessage;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ProvisioningTimeoutIntegrationTest {

  private static final DropwizardAppExtension<Configuration> DROPWIZARD_APP_EXTENSION =
      new DropwizardAppExtension<>(TestApplication.class);


  private WebSocketClient client;

  @BeforeEach
  void setUp() throws Exception {
    client = new WebSocketClient();
    client.start();
    final TestApplication testApplication = DROPWIZARD_APP_EXTENSION.getApplication();
    reset(testApplication.scheduler);
  }

  @AfterEach
  void tearDown() throws Exception {
    client.stop();
  }

  public static class TestProvisioningListener extends TestWebsocketListener {
    CompletableFuture<String> provisioningAddressFuture = new CompletableFuture<>();

    @Override
    public void onWebSocketBinary(final byte[] payload, final int offset, final int length) {
      try {
        WebSocketMessage webSocketMessage = messageFactory.parseMessage(payload, offset, length);
        if (Objects.requireNonNull(webSocketMessage.getType()) == WebSocketMessage.Type.REQUEST_MESSAGE
            && webSocketMessage.getRequestMessage().getPath().equals("/v1/address")) {
          MessageProtos.ProvisioningAddress provisioningAddress =
              MessageProtos.ProvisioningAddress.parseFrom(webSocketMessage.getRequestMessage().getBody().orElseThrow());
          provisioningAddressFuture.complete(provisioningAddress.getAddress());
          return;
        }
      } catch (InvalidMessageException e) {
        throw new RuntimeException(e);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      super.onWebSocketBinary(payload, offset, length);
    }
  }

  public static class TestApplication extends Application<Configuration> {

    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    @Override
    public void run(final Configuration configuration, final Environment environment) throws Exception {
      final WebSocketConfiguration webSocketConfiguration = new WebSocketConfiguration();
      final WebSocketEnvironment<AuthenticatedDevice> webSocketEnvironment =
          new WebSocketEnvironment<>(environment, webSocketConfiguration);

      environment.servlets()
          .addFilter("RemoteAddressFilter", new RemoteAddressFilter())
          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");
      webSocketEnvironment.setConnectListener(
          new ProvisioningConnectListener(mock(ProvisioningManager.class), scheduler, Duration.ofSeconds(5)));

      final WebSocketResourceProviderFactory<AuthenticatedDevice> webSocketServlet =
          new WebSocketResourceProviderFactory<>(webSocketEnvironment, AuthenticatedDevice.class,
              webSocketConfiguration, REMOTE_ADDRESS_ATTRIBUTE_NAME);

      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), null);
      final ServletRegistration.Dynamic websocketServlet = environment.servlets()
          .addServlet("WebSocket", webSocketServlet);
      websocketServlet.addMapping("/websocket");
      websocketServlet.setAsyncSupported(true);
    }
  }

  @Test
  public void websocketTimeoutWithHeader() throws IOException {
    final TestProvisioningListener testWebsocketListener = new TestProvisioningListener();

    final TestApplication testApplication = DROPWIZARD_APP_EXTENSION.getApplication();
    when(testApplication.scheduler.schedule(any(Runnable.class), anyLong(), any()))
        .thenReturn(mock(ScheduledFuture.class));

    final ClientUpgradeRequest upgradeRequest = new ClientUpgradeRequest();
    try (Session ignored = client.connect(testWebsocketListener,
        URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())),
        upgradeRequest).join()) {

      assertThat(testWebsocketListener.provisioningAddressFuture.join()).isNotNull();
      assertThat(testWebsocketListener.closeFuture()).isNotDone();

      final ArgumentCaptor<Runnable> closeFunctionCaptor = ArgumentCaptor.forClass(Runnable.class);
      verify(testApplication.scheduler).schedule(closeFunctionCaptor.capture(), anyLong(), any());
      closeFunctionCaptor.getValue().run();

      assertThat(testWebsocketListener.closeFuture())
          .succeedsWithin(Duration.ofSeconds(1))
          .isEqualTo(1000);
    }
  }

  @Test
  public void websocketTimeoutCancelled() throws IOException {
    final TestProvisioningListener testWebsocketListener = new TestProvisioningListener();

    final TestApplication testApplication = DROPWIZARD_APP_EXTENSION.getApplication();
    @SuppressWarnings("unchecked") final ScheduledFuture<Void> scheduled = mock(ScheduledFuture.class);
    doReturn(scheduled).when(testApplication.scheduler).schedule(any(Runnable.class), anyLong(), any());

    final ClientUpgradeRequest upgradeRequest = new ClientUpgradeRequest();
    final Session session = client.connect(testWebsocketListener,
        URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())),
        upgradeRequest).join();

    // Close the websocket, make sure the timeout is cancelled.
    session.close();
    assertThat(testWebsocketListener.closeFuture()).succeedsWithin(Duration.ofSeconds(1));
    verify(scheduled, times(1)).cancel(anyBoolean());
  }
}
