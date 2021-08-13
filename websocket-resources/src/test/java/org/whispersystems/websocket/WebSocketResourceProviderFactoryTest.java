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
import java.io.IOException;
import java.security.Principal;
import java.util.Optional;
import javax.security.auth.Subject;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.configuration.WebSocketConfiguration;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

public class WebSocketResourceProviderFactoryTest {

  private ResourceConfig jerseyEnvironment;
  private WebSocketEnvironment<Account> environment;
  private WebSocketAuthenticator<Account> authenticator;
  private ServletUpgradeRequest request;
  private ServletUpgradeResponse response;

  @BeforeEach
  void setup() {
    jerseyEnvironment = new DropwizardResourceConfig();
    //noinspection unchecked
    environment = mock(WebSocketEnvironment.class);
    //noinspection unchecked
    authenticator = mock(WebSocketAuthenticator.class);
    request = mock(ServletUpgradeRequest.class);
    response = mock(ServletUpgradeResponse.class);

  }

  @Test
  void testUnauthorized() throws AuthenticationException, IOException {
    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenReturn(
        new WebSocketAuthenticator.AuthenticationResult<>(Optional.empty(), true));
    when(environment.jersey()).thenReturn(jerseyEnvironment);

    WebSocketResourceProviderFactory<?> factory = new WebSocketResourceProviderFactory<>(environment, Account.class,
        mock(WebSocketConfiguration.class));
    Object connection = factory.createWebSocket(request, response);

    assertNull(connection);
    verify(response).sendForbidden(eq("Unauthorized"));
    verify(authenticator).authenticate(eq(request));
  }

  @Test
  void testValidAuthorization() throws AuthenticationException {
    Session session = mock(Session.class);
    Account account = new Account();

    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenReturn(
        new WebSocketAuthenticator.AuthenticationResult<>(Optional.of(account), true));
    when(environment.jersey()).thenReturn(jerseyEnvironment);
    when(session.getUpgradeRequest()).thenReturn(mock(UpgradeRequest.class));

    WebSocketResourceProviderFactory<?> factory = new WebSocketResourceProviderFactory<>(environment, Account.class,
        mock(WebSocketConfiguration.class));
    Object connection = factory.createWebSocket(request, response);

    assertNotNull(connection);
    verifyNoMoreInteractions(response);
    verify(authenticator).authenticate(eq(request));

    ((WebSocketResourceProvider<?>) connection).onWebSocketConnect(session);

    assertNotNull(((WebSocketResourceProvider<?>) connection).getContext().getAuthenticated());
    assertEquals(((WebSocketResourceProvider<?>) connection).getContext().getAuthenticated(), account);
  }

  @Test
  void testErrorAuthorization() throws AuthenticationException, IOException {
    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenThrow(new AuthenticationException("database failure"));
    when(environment.jersey()).thenReturn(jerseyEnvironment);

    WebSocketResourceProviderFactory<Account> factory = new WebSocketResourceProviderFactory<>(environment,
        Account.class,
        mock(WebSocketConfiguration.class));
    Object connection = factory.createWebSocket(request, response);

    assertNull(connection);
    verify(response).sendError(eq(500), eq("Failure"));
    verify(authenticator).authenticate(eq(request));
  }

  @Test
  void testConfigure() {
    WebSocketServletFactory servletFactory = mock(WebSocketServletFactory.class);
    when(environment.jersey()).thenReturn(jerseyEnvironment);
    when(servletFactory.getPolicy()).thenReturn(mock(WebSocketPolicy.class));

    WebSocketResourceProviderFactory<Account> factory = new WebSocketResourceProviderFactory<>(environment,
        Account.class,
        mock(WebSocketConfiguration.class));
    factory.configure(servletFactory);

    verify(servletFactory).setCreator(eq(factory));
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
