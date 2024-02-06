package org.whispersystems.textsecuregcm;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.filters.RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME;

import io.dropwizard.auth.Auth;
import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import javax.servlet.DispatcherType;
import javax.servlet.ServletRegistration;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.glassfish.jersey.server.ManagedAsync;
import org.glassfish.jersey.server.ServerProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.storage.RefreshingAccountNotFoundException;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.websocket.ReusableAuth;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.auth.PrincipalSupplier;
import org.whispersystems.websocket.auth.ReadOnly;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

@ExtendWith(DropwizardExtensionsSupport.class)
public class WebsocketReuseAuthIntegrationTest {

  private static final AuthenticatedAccount ACCOUNT = mock(AuthenticatedAccount.class);
  @SuppressWarnings("unchecked")
  private static final PrincipalSupplier<AuthenticatedAccount> PRINCIPAL_SUPPLIER = mock(PrincipalSupplier.class);
  private static final DropwizardAppExtension<Configuration> DROPWIZARD_APP_EXTENSION =
      new DropwizardAppExtension<>(TestApplication.class);


  private WebSocketClient client;

  @BeforeEach
  void setUp() throws Exception {
    reset(PRINCIPAL_SUPPLIER);
    reset(ACCOUNT);
    when(ACCOUNT.getName()).thenReturn("original");
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

      final WebSocketEnvironment<AuthenticatedAccount> webSocketEnvironment =
          new WebSocketEnvironment<>(environment, webSocketConfiguration);

      environment.jersey().register(testController);
      environment.servlets()
          .addFilter("RemoteAddressFilter", new RemoteAddressFilter(true))
          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");
      webSocketEnvironment.jersey().register(testController);
      webSocketEnvironment.jersey().register(new RemoteAddressFilter(true));
      webSocketEnvironment.setAuthenticator(upgradeRequest -> ReusableAuth.authenticated(ACCOUNT, PRINCIPAL_SUPPLIER));

      webSocketEnvironment.jersey().property(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE);
      webSocketEnvironment.setConnectListener(webSocketSessionContext -> {
      });

      final WebSocketResourceProviderFactory<AuthenticatedAccount> webSocketServlet =
          new WebSocketResourceProviderFactory<>(webSocketEnvironment, AuthenticatedAccount.class,
              webSocketConfiguration, REMOTE_ADDRESS_ATTRIBUTE_NAME);

      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), null);

      final ServletRegistration.Dynamic websocketServlet =
          environment.servlets().addServlet("WebSocket", webSocketServlet);

      websocketServlet.addMapping("/websocket");
      websocketServlet.setAsyncSupported(true);
    }
  }

  private WebSocketResponseMessage make1WebsocketRequest(final String requestPath) throws IOException {

    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();

    client.connect(testWebsocketListener,
        URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())));
    return testWebsocketListener.doGet(requestPath).join();
  }

  @ParameterizedTest
  @ValueSource(strings = {"/test/read-auth", "/test/optional-read-auth"})
  public void readAuth(final String path) throws IOException {
    final WebSocketResponseMessage response = make1WebsocketRequest(path);
    assertThat(response.getStatus()).isEqualTo(200);
    verifyNoMoreInteractions(PRINCIPAL_SUPPLIER);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/test/write-auth", "/test/optional-write-auth"})
  public void writeAuth(final String path) throws IOException {
    final AuthenticatedAccount copiedAccount = mock(AuthenticatedAccount.class);
    when(copiedAccount.getName()).thenReturn("copy");
    when(PRINCIPAL_SUPPLIER.deepCopy(any())).thenReturn(copiedAccount);

    final WebSocketResponseMessage response = make1WebsocketRequest(path);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getBody().map(String::new)).get().isEqualTo("copy");
    verify(PRINCIPAL_SUPPLIER, times(1)).deepCopy(any());
    verifyNoMoreInteractions(PRINCIPAL_SUPPLIER);
  }

  @Test
  public void readAfterWrite() throws IOException {
    when(PRINCIPAL_SUPPLIER.deepCopy(any())).thenReturn(ACCOUNT);
    final AuthenticatedAccount account2 = mock(AuthenticatedAccount.class);
    when(account2.getName()).thenReturn("refresh");
    when(PRINCIPAL_SUPPLIER.refresh(any())).thenReturn(account2);

    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();
    client.connect(testWebsocketListener,
        URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())));

    final WebSocketResponseMessage readResponse = testWebsocketListener.doGet("/test/read-auth").join();
    assertThat(readResponse.getBody().map(String::new)).get().isEqualTo("original");

    final WebSocketResponseMessage writeResponse = testWebsocketListener.doGet("/test/write-auth").join();
    assertThat(writeResponse.getBody().map(String::new)).get().isEqualTo("original");

    final WebSocketResponseMessage readResponse2 = testWebsocketListener.doGet("/test/read-auth").join();
    assertThat(readResponse2.getBody().map(String::new)).get().isEqualTo("refresh");
  }

  @Test
  public void readAfterWriteRefreshFails() throws IOException {
    when(PRINCIPAL_SUPPLIER.deepCopy(any())).thenReturn(ACCOUNT);
    when(PRINCIPAL_SUPPLIER.refresh(any())).thenThrow(RefreshingAccountNotFoundException.class);

    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();
    client.connect(testWebsocketListener,
        URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())));

    final WebSocketResponseMessage writeResponse = testWebsocketListener.doGet("/test/write-auth").join();
    assertThat(writeResponse.getBody().map(String::new)).get().isEqualTo("original");

    final WebSocketResponseMessage readResponse2 = testWebsocketListener.doGet("/test/read-auth").join();
    assertThat(readResponse2.getStatus()).isEqualTo(500);
  }

  @Test
  public void readConcurrentWithWrite() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    final AuthenticatedAccount deepCopy = mock(AuthenticatedAccount.class);
    when(deepCopy.getName()).thenReturn("deepCopy");
    when(PRINCIPAL_SUPPLIER.deepCopy(any())).thenReturn(deepCopy);

    final AuthenticatedAccount refresh = mock(AuthenticatedAccount.class);
    when(refresh.getName()).thenReturn("refresh");
    when(PRINCIPAL_SUPPLIER.refresh(any())).thenReturn(refresh);

    final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();
    client.connect(testWebsocketListener,
        URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())));

    // start a write request that takes a while to finish
    final CompletableFuture<WebSocketResponseMessage> writeResponse =
        testWebsocketListener.doGet("/test/start-delayed-write/foo");

    // send a bunch of reads, they should reflect the original auth
    final List<CompletableFuture<WebSocketResponseMessage>> futures = IntStream.range(0, 10)
        .boxed().map(i -> testWebsocketListener.doGet("/test/read-auth"))
        .toList();
    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    for (CompletableFuture<WebSocketResponseMessage> future : futures) {
      assertThat(future.join().getBody().map(String::new)).get().isEqualTo("original");
    }

    assertThat(writeResponse.isDone()).isFalse();

    // finish the delayed write request
    testWebsocketListener.doGet("/test/finish-delayed-write/foo").get(1, TimeUnit.SECONDS);
    assertThat(writeResponse.join().getBody().map(String::new)).get().isEqualTo("deepCopy");

    // subsequent reads should have the refreshed auth
    final WebSocketResponseMessage readResponse = testWebsocketListener.doGet("/test/read-auth").join();
    assertThat(readResponse.getBody().map(String::new)).get().isEqualTo("refresh");
  }


  @Path("/test")
  public static class TestController {

    private final ConcurrentHashMap<String, CountDownLatch> delayedWriteLatches = new ConcurrentHashMap<>();

    @GET
    @Path("/read-auth")
    @ManagedAsync
    public String readAuth(@ReadOnly @Auth final AuthenticatedAccount account) {
      return account.getName();
    }

    @GET
    @Path("/optional-read-auth")
    @ManagedAsync
    public String optionalReadAuth(@ReadOnly @Auth final Optional<AuthenticatedAccount> account) {
      return account.map(AuthenticatedAccount::getName).orElse("empty");
    }

    @GET
    @Path("/write-auth")
    @ManagedAsync
    public String writeAuth(@Auth final AuthenticatedAccount account) {
      return account.getName();
    }

    @GET
    @Path("/optional-write-auth")
    @ManagedAsync
    public String optionalWriteAuth(@Auth final Optional<AuthenticatedAccount> account) {
      return account.map(AuthenticatedAccount::getName).orElse("empty");
    }

    @GET
    @Path("/start-delayed-write/{id}")
    @ManagedAsync
    public String startDelayedWrite(@Auth final AuthenticatedAccount account, @PathParam("id") String id)
        throws InterruptedException {
      delayedWriteLatches.computeIfAbsent(id, i -> new CountDownLatch(1)).await();
      return account.getName();
    }

    @GET
    @Path("/finish-delayed-write/{id}")
    @ManagedAsync
    public String finishDelayedWrite(@PathParam("id") String id) {
      delayedWriteLatches.computeIfAbsent(id, i -> new CountDownLatch(1)).countDown();
      return "ok";
    }
  }
}
