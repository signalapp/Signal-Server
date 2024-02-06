/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.filters.RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.UUID;
import javax.servlet.DispatcherType;
import javax.servlet.ServletRegistration;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.client.Invocation;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.glassfish.jersey.server.ManagedAsync;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.TestWebsocketListener;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.websocket.WebSocketResourceProviderFactory;
import org.whispersystems.websocket.auth.PrincipalSupplier;
import org.whispersystems.websocket.auth.ReadOnly;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

@ExtendWith(DropwizardExtensionsSupport.class)
class PhoneNumberChangeRefreshRequirementProviderTest {

  private static final String NUMBER = "+18005551234";
  private static final String CHANGED_NUMBER = "+18005554321";
  private static final String TEST_CRED_HEADER = HeaderUtils.basicAuthHeader("test", "password");


  private static final DropwizardAppExtension<Configuration> DROPWIZARD_APP_EXTENSION = new DropwizardAppExtension<>(
      TestApplication.class);

  private static final AccountAuthenticator AUTHENTICATOR = mock(AccountAuthenticator.class);
  private static final AccountsManager ACCOUNTS_MANAGER = mock(AccountsManager.class);
  private static final ClientPresenceManager CLIENT_PRESENCE = mock(ClientPresenceManager.class);

  private WebSocketClient client;
  private final Account account1 = new Account();
  private final Account account2 = new Account();
  private final Device authenticatedDevice = DevicesHelper.createDevice(Device.PRIMARY_ID);


  @BeforeEach
  void setUp() throws Exception {
    reset(AUTHENTICATOR, CLIENT_PRESENCE, ACCOUNTS_MANAGER);
    client = new WebSocketClient();
    client.start();

    final UUID uuid = UUID.randomUUID();
    account1.setUuid(uuid);
    account1.addDevice(authenticatedDevice);
    account1.setNumber(NUMBER, UUID.randomUUID());

    account2.setUuid(uuid);
    account2.addDevice(authenticatedDevice);
    account2.setNumber(CHANGED_NUMBER, UUID.randomUUID());

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
      webSocketEnvironment.jersey().register(testController);
      environment.servlets()
          .addFilter("RemoteAddressFilter", new RemoteAddressFilter(true))
          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");
      webSocketEnvironment.jersey().register(new RemoteAddressFilter(true));
      webSocketEnvironment.jersey()
          .register(new WebsocketRefreshApplicationEventListener(ACCOUNTS_MANAGER, CLIENT_PRESENCE));
      environment.jersey()
          .register(new WebsocketRefreshApplicationEventListener(ACCOUNTS_MANAGER, CLIENT_PRESENCE));
      webSocketEnvironment.setConnectListener(webSocketSessionContext -> {
      });


      environment.jersey().register(new AuthDynamicFeature(new BasicCredentialAuthFilter.Builder<AuthenticatedAccount>()
          .setAuthenticator(AUTHENTICATOR)
          .buildAuthFilter()));
      webSocketEnvironment.setAuthenticator(new WebSocketAccountAuthenticator(AUTHENTICATOR, mock(PrincipalSupplier.class)));

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

  enum Protocol { HTTP, WEBSOCKET }

  private void makeAnonymousRequest(final Protocol protocol, final String requestPath) throws IOException {
    makeRequest(protocol, requestPath, true);
  }

  /*
   * Make an authenticated request that will return account1 as the principal
   */
  private void makeAuthenticatedRequest(
      final Protocol protocol,
      final String requestPath) throws IOException {
    when(AUTHENTICATOR.authenticate(any())).thenReturn(Optional.of(new AuthenticatedAccount(account1, authenticatedDevice)));
    makeRequest(protocol,requestPath, false);
  }

  private void makeRequest(final Protocol protocol, final String requestPath, final boolean anonymous) throws IOException {
    switch (protocol) {
      case WEBSOCKET -> {
        final TestWebsocketListener testWebsocketListener = new TestWebsocketListener();
        final ClientUpgradeRequest upgradeRequest = new ClientUpgradeRequest();
        if (!anonymous) {
          upgradeRequest.setHeader(HttpHeaders.AUTHORIZATION, TEST_CRED_HEADER);
        }
        client.connect(
            testWebsocketListener,
            URI.create(String.format("ws://127.0.0.1:%d/websocket", DROPWIZARD_APP_EXTENSION.getLocalPort())),
            upgradeRequest);
        testWebsocketListener.sendRequest(requestPath, "GET", Collections.emptyList(), Optional.empty()).join();
      }
      case HTTP -> {
        final Invocation.Builder request = DROPWIZARD_APP_EXTENSION.client()
            .target("http://127.0.0.1:%s%s".formatted(DROPWIZARD_APP_EXTENSION.getLocalPort(), requestPath))
            .request();
        if (!anonymous) {
          request.header(HttpHeaders.AUTHORIZATION, TEST_CRED_HEADER);
        }
        request.get();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void handleRequestNoChange(final Protocol protocol) throws IOException {
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(any())).thenReturn(Optional.of(account1));
    makeAuthenticatedRequest(protocol, "/test/annotated");

    // Event listeners can fire after responses are sent
    verify(ACCOUNTS_MANAGER, timeout(5000).times(1)).getByAccountIdentifier(eq(account1.getUuid()));
    verifyNoMoreInteractions(CLIENT_PRESENCE);
    verifyNoMoreInteractions(ACCOUNTS_MANAGER);
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void handleRequestChange(final Protocol protocol) throws IOException {
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(any())).thenReturn(Optional.of(account2));
    when(AUTHENTICATOR.authenticate(any())).thenReturn(Optional.of(new AuthenticatedAccount(account1, authenticatedDevice)));

    makeAuthenticatedRequest(protocol, "/test/annotated");

    // Make sure we disconnect the account if the account has changed numbers. Event listeners can fire after responses
    // are sent, so use a timeout.
    verify(CLIENT_PRESENCE, timeout(5000))
        .disconnectPresence(eq(account1.getUuid()), eq(authenticatedDevice.getId()));
    verifyNoMoreInteractions(CLIENT_PRESENCE);
  }

  @Test
  void handleRequestChangeAsyncEndpoint() throws IOException {
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(any())).thenReturn(Optional.of(account2));
    when(AUTHENTICATOR.authenticate(any())).thenReturn(Optional.of(new AuthenticatedAccount(account1, authenticatedDevice)));

    // Event listeners with asynchronous HTTP endpoints don't currently correctly maintain state between request and
    // response
    makeAuthenticatedRequest(Protocol.WEBSOCKET, "/test/async-annotated");

    // Make sure we disconnect the account if the account has changed numbers. Event listeners can fire after responses
    // are sent, so use a timeout.
    verify(CLIENT_PRESENCE, timeout(5000))
        .disconnectPresence(eq(account1.getUuid()), eq(authenticatedDevice.getId()));
    verifyNoMoreInteractions(CLIENT_PRESENCE);
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void handleRequestNotAnnotated(final Protocol protocol) throws IOException, InterruptedException {
    makeAuthenticatedRequest(protocol,"/test/not-annotated");

    // Give a tick for event listeners to run. Racy, but should occasionally catch an errant running listener if one is
    // introduced.
    Thread.sleep(100);

    // Shouldn't even read the account if the method has not been annotated
    verifyNoMoreInteractions(ACCOUNTS_MANAGER);
    verifyNoMoreInteractions(CLIENT_PRESENCE);
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void handleRequestNotAuthenticated(final Protocol protocol) throws IOException, InterruptedException {
    makeAnonymousRequest(protocol, "/test/not-authenticated");

    // Give a tick for event listeners to run. Racy, but should occasionally catch an errant running listener if one is
    // introduced.
    Thread.sleep(100);

    // Shouldn't even read the account if the method has not been annotated
    verifyNoMoreInteractions(ACCOUNTS_MANAGER);
    verifyNoMoreInteractions(CLIENT_PRESENCE);
  }


  @Path("/test")
  public static class TestController {

    @GET
    @Path("/annotated")
    @ChangesPhoneNumber
    public String annotated(@ReadOnly @Auth final AuthenticatedAccount account) {
      return "ok";
    }

    @GET
    @Path("/async-annotated")
    @ChangesPhoneNumber
    @ManagedAsync
    public String asyncAnnotated(@ReadOnly @Auth final AuthenticatedAccount account) {
      return "ok";
    }

    @GET
    @Path("/not-authenticated")
    @ChangesPhoneNumber
    public String notAuthenticated() {
      return "ok";
    }

    @GET
    @Path("/not-annotated")
    public String notAnnotated(@ReadOnly @Auth final AuthenticatedAccount account) {
      return "ok";
    }
  }
}
