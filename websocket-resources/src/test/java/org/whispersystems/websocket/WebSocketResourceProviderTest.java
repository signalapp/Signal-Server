package org.whispersystems.websocket;

import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.websocket.api.CloseStatus;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.auth.AuthenticationException;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class WebSocketResourceProviderTest {

  @Test
  public void testOnConnect() throws AuthenticationException, IOException {
    HttpServlet                    contextHandler = mock(HttpServlet.class);
    WebSocketAuthenticator<String> authenticator  = mock(WebSocketAuthenticator.class);
    RequestLog                     requestLog     = mock(RequestLog.class);
    WebSocketResourceProvider provider       = new WebSocketResourceProvider(contextHandler, requestLog,
                                                                             null,
                                                                             new ProtobufWebSocketMessageFactory(),
                                                                             Optional.empty(),
                                                                             30000);

    Session        session = mock(Session.class       );
    UpgradeRequest request = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(authenticator.authenticate(request)).thenReturn(new WebSocketAuthenticator.AuthenticationResult<>(Optional.of("fooz"), true));

    provider.onWebSocketConnect(session);

    verify(session, never()).close(anyInt(), anyString());
    verify(session, never()).close();
    verify(session, never()).close(any(CloseStatus.class));
  }

  @Test
  public void testRouteMessage() throws Exception {
    HttpServlet                    servlet       = mock(HttpServlet.class           );
    WebSocketAuthenticator<String> authenticator = mock(WebSocketAuthenticator.class);
    RequestLog                     requestLog    = mock(RequestLog.class            );
    WebSocketResourceProvider      provider      = new WebSocketResourceProvider(servlet, requestLog, Optional.of((WebSocketAuthenticator)authenticator), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);
    when(authenticator.authenticate(request)).thenReturn(new WebSocketAuthenticator.AuthenticationResult<>(Optional.of("foo"), true));

    provider.onWebSocketConnect(session);

    verify(session, never()).close(anyInt(), anyString());
    verify(session, never()).close();
    verify(session, never()).close(any(CloseStatus.class));

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/bar", new LinkedList<String>(), Optional.of("hello world!".getBytes())).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<HttpServletRequest> requestCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);

    verify(servlet).service(requestCaptor.capture(), any(HttpServletResponse.class));

    HttpServletRequest bundledRequest = requestCaptor.getValue();

    byte[] expected = new byte[bundledRequest.getInputStream().available()];
    int    read     = bundledRequest.getInputStream().read(expected);

    assertThat(read).isEqualTo(expected.length);
    assertThat(new String(expected)).isEqualTo("hello world!");
  }

}
