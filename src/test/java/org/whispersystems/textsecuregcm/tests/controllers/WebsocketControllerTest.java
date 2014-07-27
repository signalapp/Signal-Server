package org.whispersystems.textsecuregcm.tests.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.eclipse.jetty.websocket.api.CloseStatus;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.controllers.WebsocketController;
import org.whispersystems.textsecuregcm.entities.AcknowledgeWebsocketMessage;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.PendingMessage;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.StoredMessages;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;
import org.whispersystems.textsecuregcm.websocket.WebsocketControllerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.auth.basic.BasicCredentials;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class WebsocketControllerTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String VALID_USER   = "+14152222222";
  private static final String INVALID_USER = "+14151111111";

  private static final String VALID_PASSWORD   = "secure";
  private static final String INVALID_PASSWORD = "insecure";

  private static final StoredMessages       storedMessages       = mock(StoredMessages.class);
  private static final AccountAuthenticator accountAuthenticator = mock(AccountAuthenticator.class);
  private static final AccountsManager      accountsManager      = mock(AccountsManager.class);
  private static final PubSubManager        pubSubManager        = mock(PubSubManager.class       );
  private static final Account              account              = mock(Account.class             );
  private static final Device               device               = mock(Device.class              );
  private static final UpgradeRequest       upgradeRequest       = mock(UpgradeRequest.class      );
  private static final Session              session              = mock(Session.class             );
  private static final PushSender           pushSender           = mock(PushSender.class);

  @Test
  public void testCredentials() throws Exception {
    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(account));

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(INVALID_USER, INVALID_PASSWORD))))
        .thenReturn(Optional.<Account>absent());

    when(session.getUpgradeRequest()).thenReturn(upgradeRequest);

    WebsocketController controller = new WebsocketController(accountAuthenticator, accountsManager, pushSender, pubSubManager, storedMessages);

    when(upgradeRequest.getParameterMap()).thenReturn(new HashMap<String, String[]>() {{
      put("login", new String[] {VALID_USER});
      put("password", new String[] {VALID_PASSWORD});
    }});

    controller.onWebSocketConnect(session);

    verify(session, never()).close();
    verify(session, never()).close(any(CloseStatus.class));
    verify(session, never()).close(anyInt(), anyString());

    when(upgradeRequest.getParameterMap()).thenReturn(new HashMap<String, String[]>() {{
      put("login", new String[] {INVALID_USER});
      put("password", new String[] {INVALID_PASSWORD});
    }});

    controller.onWebSocketConnect(session);

    verify(session).close(any(CloseStatus.class));
  }

  @Test
  public void testOpen() throws Exception {
    RemoteEndpoint remote = mock(RemoteEndpoint.class);

    List<PendingMessage> outgoingMessages = new LinkedList<PendingMessage>() {{
      add(new PendingMessage("sender1", 1111, false, "first"));
      add(new PendingMessage("sender1", 2222, false, "second"));
      add(new PendingMessage("sender2", 3333, false, "third"));
    }};

    when(device.getId()).thenReturn(2L);
    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));
    when(account.getNumber()).thenReturn("+14152222222");
    when(session.getRemote()).thenReturn(remote);
    when(session.getUpgradeRequest()).thenReturn(upgradeRequest);

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = new LinkedList<Device>() {{
      add(sender1device);
    }};

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.get("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.get("sender2")).thenReturn(Optional.<Account>absent());

    when(upgradeRequest.getParameterMap()).thenReturn(new HashMap<String, String[]>() {{
      put("login", new String[] {VALID_USER});
      put("password", new String[] {VALID_PASSWORD});
    }});

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(account));

    when(storedMessages.getMessagesForDevice(new WebsocketAddress(account.getNumber(), device.getId())))
        .thenReturn(outgoingMessages);

    WebsocketControllerFactory factory    = new WebsocketControllerFactory(accountAuthenticator, accountsManager, pushSender, storedMessages, pubSubManager);
    WebsocketController        controller = (WebsocketController) factory.createWebSocket(null, null);

    controller.onWebSocketConnect(session);

    verify(pubSubManager).subscribe(eq(new WebsocketAddress("+14152222222", 2L)), eq((controller)));
    verify(remote, times(3)).sendStringByFuture(anyString());

    controller.onWebSocketText(mapper.writeValueAsString(new AcknowledgeWebsocketMessage(1)));
    controller.onWebSocketClose(1000, "Closed");

    List<PendingMessage> pending = new LinkedList<PendingMessage>() {{
      add(new PendingMessage("sender1", 1111, false, "first"));
      add(new PendingMessage("sender2", 3333, false, "third"));
    }};

    verify(pushSender, times(2)).sendMessage(eq(account), eq(device), any(PendingMessage.class));
    verify(pushSender, times(1)).sendMessage(eq(sender1), eq(sender1device), any(MessageProtos.OutgoingMessageSignal.class));
  }

}
