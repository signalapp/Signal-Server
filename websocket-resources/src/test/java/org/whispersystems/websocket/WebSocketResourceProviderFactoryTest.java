package org.whispersystems.websocket;


import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.junit.Test;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.setup.WebSocketEnvironment;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Optional;

import io.dropwizard.jersey.setup.JerseyEnvironment;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class WebSocketResourceProviderFactoryTest {

  @Test
  public void testUnauthorized() throws ServletException, AuthenticationException, IOException {
    JerseyEnvironment      jerseyEnvironment = mock(JerseyEnvironment.class     );
    WebSocketEnvironment   environment       = mock(WebSocketEnvironment.class  );
    WebSocketAuthenticator authenticator     = mock(WebSocketAuthenticator.class);
    ServletUpgradeRequest  request           = mock(ServletUpgradeRequest.class );
    ServletUpgradeResponse response          = mock(ServletUpgradeResponse.class);

    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenReturn(new WebSocketAuthenticator.AuthenticationResult<>(Optional.empty(), true));
    when(environment.jersey()).thenReturn(jerseyEnvironment);

    WebSocketResourceProviderFactory factory    = new WebSocketResourceProviderFactory(environment);
    Object                           connection = factory.createWebSocket(request, response);

    assertNull(connection);
    verify(response).sendForbidden(eq("Unauthorized"));
    verify(authenticator).authenticate(eq(request));
  }

  @Test
  public void testValidAuthorization() throws AuthenticationException, ServletException {
    JerseyEnvironment      jerseyEnvironment = mock(JerseyEnvironment.class     );
    WebSocketEnvironment   environment       = mock(WebSocketEnvironment.class  );
    WebSocketAuthenticator authenticator     = mock(WebSocketAuthenticator.class);
    ServletUpgradeRequest  request           = mock(ServletUpgradeRequest.class );
    ServletUpgradeResponse response          = mock(ServletUpgradeResponse.class);
    Session                session           = mock(Session.class               );
    Account                account           = new Account();

    when(environment.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(eq(request))).thenReturn(new WebSocketAuthenticator.AuthenticationResult<>(Optional.of(account), true));
    when(environment.jersey()).thenReturn(jerseyEnvironment);

    WebSocketResourceProviderFactory factory    = new WebSocketResourceProviderFactory(environment);
    Object                           connection = factory.createWebSocket(request, response);

    assertNotNull(connection);
    verifyNoMoreInteractions(response);
    verify(authenticator).authenticate(eq(request));

    ((WebSocketResourceProvider)connection).onWebSocketConnect(session);

    assertNotNull(((WebSocketResourceProvider) connection).getContext().getAuthenticated());
    assertEquals(((WebSocketResourceProvider)connection).getContext().getAuthenticated(), account);
  }

  private static class Account {}


}
