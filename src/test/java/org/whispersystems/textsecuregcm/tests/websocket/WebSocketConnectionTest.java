package org.whispersystems.textsecuregcm.tests.websocket;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.StoredMessages;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnection;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.session.WebSocketSessionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.auth.basic.BasicCredentials;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;

public class WebSocketConnectionTest {

//  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String VALID_USER   = "+14152222222";
  private static final String INVALID_USER = "+14151111111";

  private static final String VALID_PASSWORD   = "secure";
  private static final String INVALID_PASSWORD = "insecure";

//  private static final StoredMessages       storedMessages       = mock(StoredMessages.class);
  private static final AccountAuthenticator accountAuthenticator = mock(AccountAuthenticator.class);
  private static final AccountsManager      accountsManager      = mock(AccountsManager.class);
  private static final PubSubManager        pubSubManager        = mock(PubSubManager.class       );
  private static final Account              account              = mock(Account.class             );
  private static final Device               device               = mock(Device.class              );
  private static final UpgradeRequest       upgradeRequest       = mock(UpgradeRequest.class      );
//  private static final Session              session              = mock(Session.class             );
  private static final PushSender           pushSender           = mock(PushSender.class);

  @Test
  public void testCredentials() throws Exception {
    StoredMessages                storedMessages         = mock(StoredMessages.class);
    WebSocketAccountAuthenticator webSocketAuthenticator = new WebSocketAccountAuthenticator(accountAuthenticator);
    AuthenticatedConnectListener connectListener        = new AuthenticatedConnectListener(accountsManager, pushSender, storedMessages, pubSubManager);
    WebSocketSessionContext       sessionContext         = mock(WebSocketSessionContext.class);

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(account));

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(INVALID_USER, INVALID_PASSWORD))))
        .thenReturn(Optional.<Account>absent());

    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));

//    when(session.getUpgradeRequest()).thenReturn(upgradeRequest);
//
//    WebsocketController controller = new WebsocketController(accountAuthenticator, accountsManager, pushSender, pubSubManager, storedMessages);

    when(upgradeRequest.getParameterMap()).thenReturn(new HashMap<String, String[]>() {{
      put("login", new String[] {VALID_USER});
      put("password", new String[] {VALID_PASSWORD});
    }});

    Optional<Account> account = webSocketAuthenticator.authenticate(upgradeRequest);
    when(sessionContext.getAuthenticated(Account.class)).thenReturn(account);

    connectListener.onWebSocketConnect(sessionContext);

    verify(sessionContext).addListener(any(WebSocketSessionContext.WebSocketEventListener.class));

//
//    controller.onWebSocketConnect(session);

//    verify(session, never()).close();
//    verify(session, never()).close(any(CloseStatus.class));
//    verify(session, never()).close(anyInt(), anyString());

    when(upgradeRequest.getParameterMap()).thenReturn(new HashMap<String, String[]>() {{
      put("login", new String[] {INVALID_USER});
      put("password", new String[] {INVALID_PASSWORD});
    }});

    account = webSocketAuthenticator.authenticate(upgradeRequest);
    when(sessionContext.getAuthenticated(Account.class)).thenReturn(account);

    WebSocketClient client = mock(WebSocketClient.class);
    when(sessionContext.getClient()).thenReturn(client);

    connectListener.onWebSocketConnect(sessionContext);

    verify(sessionContext, times(1)).addListener(any(WebSocketSessionContext.WebSocketEventListener.class));
    verify(client).close(eq(4001), anyString());
  }

  @Test
  public void testOpen() throws Exception {
    StoredMessages storedMessages = mock(StoredMessages.class);

    List<OutgoingMessageSignal> outgoingMessages = new LinkedList<OutgoingMessageSignal>() {{
      add(createMessage("sender1", 1111, false, "first"));
      add(createMessage("sender1", 2222, false, "second"));
      add(createMessage("sender2", 3333, false, "third"));
    }};

    when(device.getId()).thenReturn(2L);
    when(device.getSignalingKey()).thenReturn(Base64.encodeBytes(new byte[52]));

    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));
    when(account.getNumber()).thenReturn("+14152222222");

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = new LinkedList<Device>() {{
      add(sender1device);
    }};

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.get("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.get("sender2")).thenReturn(Optional.<Account>absent());

    when(storedMessages.getMessagesForDevice(new WebsocketAddress(account.getNumber(), device.getId())))
        .thenReturn(outgoingMessages);

    final List<SettableFuture<WebSocketResponseMessage>> futures = new LinkedList<>();
    final WebSocketClient                                client  = mock(WebSocketClient.class);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(Optional.class)))
        .thenAnswer(new Answer<SettableFuture<WebSocketResponseMessage>>() {
          @Override
          public SettableFuture<WebSocketResponseMessage> answer(InvocationOnMock invocationOnMock) throws Throwable {
            SettableFuture<WebSocketResponseMessage> future = SettableFuture.create();
            futures.add(future);
            return future;
          }
        });

    WebSocketConnection connection = new WebSocketConnection(accountsManager, pushSender, storedMessages,
                                                             pubSubManager, account, device, client);

    connection.onConnected();

    verify(pubSubManager).subscribe(eq(new WebsocketAddress("+14152222222", 2L)), eq((connection)));
    verify(client, times(3)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(Optional.class));

    assertTrue(futures.size() == 3);

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(1).set(response);

    futures.get(0).setException(new IOException());
    futures.get(2).setException(new IOException());

    List<OutgoingMessageSignal> pending = new LinkedList<OutgoingMessageSignal>() {{
      add(createMessage("sender1", 1111, false, "first"));
      add(createMessage("sender2", 3333, false, "third"));
    }};

    verify(pushSender, times(2)).sendMessage(eq(account), eq(device), any(OutgoingMessageSignal.class));
    verify(pushSender, times(1)).sendMessage(eq(sender1), eq(sender1device), any(OutgoingMessageSignal.class));

    connection.onConnectionLost();
    verify(pubSubManager).unsubscribe(eq(new WebsocketAddress("+14152222222", 2L)), eq(connection));
  }

  private OutgoingMessageSignal createMessage(String sender, long timestamp, boolean receipt, String content) {
    return OutgoingMessageSignal.newBuilder()
                                .setSource(sender)
                                .setSourceDevice(1)
                                .setType(receipt ? OutgoingMessageSignal.Type.RECEIPT_VALUE : OutgoingMessageSignal.Type.CIPHERTEXT_VALUE)
                                .setTimestamp(timestamp)
                                .setMessage(ByteString.copyFrom(content.getBytes()))
                                .build();
  }

}
