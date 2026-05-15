/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.jersey.DropwizardResourceConfig;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;
import java.util.Optional;
import javax.security.auth.Subject;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.websocket.server.JettyServerUpgradeResponse;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.websocket.auth.InvalidCredentialsException;
import org.whispersystems.websocket.auth.AuthenticatedWebSocketUpgradeFilter;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

public class WebSocketResourceProviderFactoryTest {

  private static final String REMOTE_ADDRESS_PROPERTY_NAME = "org.whispersystems.websocket.test.remoteAddress";

  private ResourceConfig jerseyEnvironment;
  private WebSocketEnvironment<Account> environment;
  private WebSocketAuthenticator<Account> authenticator;
  private JettyServerUpgradeRequest request;
  private JettyServerUpgradeResponse response;

  @BeforeEach
  void setup() {
    jerseyEnvironment = new DropwizardResourceConfig();
    //noinspection unchecked
    environment = mock(WebSocketEnvironment.class);
    //noinspection unchecked
    authenticator = mock(WebSocketAuthenticator.class);
    request = mock(JettyServerUpgradeRequest.class);
    response = mock(JettyServerUpgradeResponse.class);

  }

  @Test
  void testUnauthorized() throws InvalidCredentialsException, IOException {
    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenThrow(new InvalidCredentialsException());
    when(environment.jersey()).thenReturn(jerseyEnvironment);

    WebSocketResourceProviderFactory<?> factory = new WebSocketResourceProviderFactory<>(environment, Account.class,
        mock(WebSocketConfiguration.class), REMOTE_ADDRESS_PROPERTY_NAME);
    Object connection = factory.createWebSocket(request, response);

    assertNull(connection);
    verify(response).sendForbidden(eq("Unauthorized"));
    verify(authenticator).authenticate(eq(request));
  }

  @Test
  void testValidAuthorization() throws InvalidCredentialsException {
    Account account = new Account();

    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request)))
        .thenReturn(Optional.of(account));
    when(environment.jersey()).thenReturn(jerseyEnvironment);
    final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getAttribute(REMOTE_ADDRESS_PROPERTY_NAME)).thenReturn("127.0.0.1");
    when(request.getHttpServletRequest()).thenReturn(httpServletRequest);

    WebSocketResourceProviderFactory<?> factory = new WebSocketResourceProviderFactory<>(environment, Account.class,
        mock(WebSocketConfiguration.class), REMOTE_ADDRESS_PROPERTY_NAME);
    Object connection = factory.createWebSocket(request, response);

    assertNotNull(connection);
    verifyNoMoreInteractions(response);
    verify(authenticator).authenticate(eq(request));

    ((WebSocketResourceProvider<?>) connection).onWebSocketConnect(mock(Session.class));

    assertNotNull(((WebSocketResourceProvider<?>) connection).getContext().getAuthenticated());
    assertEquals(((WebSocketResourceProvider<?>) connection).getContext().getAuthenticated(), account);
  }

  @Test
  void testErrorAuthorization() throws InvalidCredentialsException, IOException {
    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenThrow(new RuntimeException("database failure"));
    when(environment.jersey()).thenReturn(jerseyEnvironment);

    WebSocketResourceProviderFactory<Account> factory = new WebSocketResourceProviderFactory<>(environment,
        Account.class,
        mock(WebSocketConfiguration.class),
        REMOTE_ADDRESS_PROPERTY_NAME);
    Object connection = factory.createWebSocket(request, response);

    assertNull(connection);
    verify(response).sendError(eq(500), eq("Failure"));
    verify(authenticator).authenticate(eq(request));
  }

  @Test
  void testConfigure() {
    JettyWebSocketServletFactory servletFactory = mock(JettyWebSocketServletFactory.class);
    when(environment.jersey()).thenReturn(jerseyEnvironment);

    WebSocketResourceProviderFactory<Account> factory = new WebSocketResourceProviderFactory<>(environment,
        Account.class,
        mock(WebSocketConfiguration.class),
        REMOTE_ADDRESS_PROPERTY_NAME);
    factory.configure(servletFactory);

    verify(servletFactory).setCreator(eq(factory));
  }

  @Test
  void testAuthenticatedWebSocketUpgradeFilter() throws InvalidCredentialsException {
    final Account account = new Account();
    final Optional<Account> reusableAuth = Optional.of(account);

    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenReturn(reusableAuth);
    when(environment.jersey()).thenReturn(jerseyEnvironment);
    final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getAttribute(REMOTE_ADDRESS_PROPERTY_NAME)).thenReturn("127.0.0.1");
    when(request.getHttpServletRequest()).thenReturn(httpServletRequest);

    final AuthenticatedWebSocketUpgradeFilter<Account> filter = mock(AuthenticatedWebSocketUpgradeFilter.class);
    when(environment.getAuthenticatedWebSocketUpgradeFilter()).thenReturn(filter);

    final WebSocketResourceProviderFactory<?> factory = new WebSocketResourceProviderFactory<>(environment, Account.class,
        mock(WebSocketConfiguration.class), REMOTE_ADDRESS_PROPERTY_NAME);
    assertNotNull(factory.createWebSocket(request, response));

    verify(filter).handleAuthentication(reusableAuth, request, response);
  }


  private static class Account implements Principal {
    @Override
    public String getName() {
      return null;
    }

    @Override
    public boolean implies(Subject subject) {
      return false;
    }

  }


}
