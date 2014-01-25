package org.whispersystems.textsecuregcm.tests.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.yammer.dropwizard.auth.basic.BasicCredentials;
import org.eclipse.jetty.websocket.WebSocket;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.controllers.WebsocketController;
import org.whispersystems.textsecuregcm.controllers.WebsocketControllerFactory;
import org.whispersystems.textsecuregcm.entities.AcknowledgeWebsocketMessage;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.StoredMessageManager;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import javax.servlet.http.HttpServletRequest;

import java.util.LinkedList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class WebsocketControllerTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String VALID_USER   = "+14152222222";
  private static final String INVALID_USER = "+14151111111";

  private static final String VALID_PASSWORD   = "secure";
  private static final String INVALID_PASSWORD = "insecure";

  private final StoredMessageManager storedMessageManager = mock(StoredMessageManager.class);
  private final AccountAuthenticator accountAuthenticator = mock(AccountAuthenticator.class);
  private final PubSubManager        pubSubManager        = mock(PubSubManager.class       );
  private final Account              account              = mock(Account.class             );
  private final Device               device               = mock(Device.class              );
  private final HttpServletRequest   request              = mock(HttpServletRequest.class  );
  private final WebSocket.Connection connection           = mock(WebSocket.Connection.class);

  @Test
  public void testCredentials() throws Exception {
    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(account));

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(INVALID_USER, INVALID_PASSWORD))))
        .thenReturn(Optional.<Account>absent());

    WebsocketControllerFactory factory = new WebsocketControllerFactory(accountAuthenticator,
                                                                        storedMessageManager,
                                                                        pubSubManager);

    when(request.getParameter(eq("user"))).thenReturn(VALID_USER);
    when(request.getParameter(eq("password"))).thenReturn(VALID_PASSWORD);

    assertThat(factory.checkOrigin(request, "foobar")).isEqualTo(true);

    when(request.getParameter(eq("user"))).thenReturn(INVALID_USER);
    when(request.getParameter(eq("password"))).thenReturn(INVALID_PASSWORD);

    assertThat(factory.checkOrigin(request, "foobar")).isEqualTo(false);
  }

  @Test
  public void testOpen() throws Exception {
    List<String> outgoingMessages = new LinkedList<String>() {{
      add("first");
      add("second");
      add("third");
    }};

    when(device.getId()).thenReturn(2L);
    when(account.getId()).thenReturn(31337L);
    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));

    when(request.getParameter(eq("user"))).thenReturn(VALID_USER);
    when(request.getParameter(eq("password"))).thenReturn(VALID_PASSWORD);

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(account));

    when(storedMessageManager.getOutgoingMessages(eq(account), eq(device))).thenReturn(outgoingMessages);

    WebsocketControllerFactory factory = new WebsocketControllerFactory(accountAuthenticator,
                                                                        storedMessageManager,
                                                                        pubSubManager);

    assertThat(factory.checkOrigin(request, "foobar")).isEqualTo(true);

    WebsocketController socket = (WebsocketController)factory.doWebSocketConnect(request, "foo");
    socket.onOpen(connection);

    verify(pubSubManager).subscribe(eq(new WebsocketAddress(31337L, 2L)), eq((socket)));
    verify(connection, times(3)).sendMessage(anyString());

    socket.onMessage(mapper.writeValueAsString(new AcknowledgeWebsocketMessage(1)));
    socket.onClose(1000, "Closed");

    List<String> pending = new LinkedList<String>() {{
      add("first");
      add("third");
    }};

    verify(storedMessageManager).storeMessages(eq(account), eq(device), eq(pending));
  }

}
