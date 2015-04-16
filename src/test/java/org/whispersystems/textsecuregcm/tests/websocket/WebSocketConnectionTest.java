package org.whispersystems.textsecuregcm.tests.websocket;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.websocket.AuthenticatedConnectListener;
import org.whispersystems.textsecuregcm.websocket.WebSocketAccountAuthenticator;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnection;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import io.dropwizard.auth.basic.BasicCredentials;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;

public class WebSocketConnectionTest {

  private static final String VALID_USER   = "+14152222222";
  private static final String INVALID_USER = "+14151111111";

  private static final String VALID_PASSWORD   = "secure";
  private static final String INVALID_PASSWORD = "insecure";

  private static final AccountAuthenticator accountAuthenticator = mock(AccountAuthenticator.class);
  private static final AccountsManager      accountsManager      = mock(AccountsManager.class);
  private static final PubSubManager        pubSubManager        = mock(PubSubManager.class       );
  private static final Account              account              = mock(Account.class             );
  private static final Device               device               = mock(Device.class              );
  private static final UpgradeRequest       upgradeRequest       = mock(UpgradeRequest.class      );
  private static final PushSender           pushSender           = mock(PushSender.class);
  private static final ReceiptSender        receiptSender        = mock(ReceiptSender.class);

  @Test
  public void testCredentials() throws Exception {
    MessagesManager               storedMessages         = mock(MessagesManager.class);
    WebSocketAccountAuthenticator webSocketAuthenticator = new WebSocketAccountAuthenticator(accountAuthenticator);
    AuthenticatedConnectListener  connectListener        = new AuthenticatedConnectListener(accountsManager, pushSender, receiptSender, storedMessages, pubSubManager);
    WebSocketSessionContext       sessionContext         = mock(WebSocketSessionContext.class);

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(account));

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(INVALID_USER, INVALID_PASSWORD))))
        .thenReturn(Optional.<Account>absent());

    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));

    when(upgradeRequest.getParameterMap()).thenReturn(new HashMap<String, String[]>() {{
      put("login", new String[] {VALID_USER});
      put("password", new String[] {VALID_PASSWORD});
    }});

    Optional<Account> account = webSocketAuthenticator.authenticate(upgradeRequest);
    when(sessionContext.getAuthenticated(Account.class)).thenReturn(account);

    connectListener.onWebSocketConnect(sessionContext);

    verify(sessionContext).addListener(any(WebSocketSessionContext.WebSocketEventListener.class));

    when(upgradeRequest.getParameterMap()).thenReturn(new HashMap<String, String[]>() {{
      put("login", new String[] {INVALID_USER});
      put("password", new String[] {INVALID_PASSWORD});
    }});

    account = webSocketAuthenticator.authenticate(upgradeRequest);
    assertFalse(account.isPresent());
  }

  @Test
  public void testOpen() throws Exception {
    MessagesManager storedMessages = mock(MessagesManager.class);

    List<OutgoingMessageEntity> outgoingMessages = new LinkedList<OutgoingMessageEntity> () {{
      add(createMessage(1L, "sender1", 1111, false, "first"));
      add(createMessage(2L, "sender1", 2222, false, "second"));
      add(createMessage(3L, "sender2", 3333, false, "third"));
    }};

    when(device.getId()).thenReturn(2L);
    when(device.getSignalingKey()).thenReturn(Base64.encodeBytes(new byte[52]));

    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));
    when(account.getNumber()).thenReturn("+14152222222");

    final Device sender1device = mock(Device.class);

    Set<Device> sender1devices = new HashSet<Device>() {{
      add(sender1device);
    }};

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.get("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.get("sender2")).thenReturn(Optional.<Account>absent());

    when(storedMessages.getMessagesForDevice(account.getNumber(), device.getId()))
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

    WebsocketAddress websocketAddress = new WebsocketAddress(account.getNumber(), device.getId());
    WebSocketConnection connection = new WebSocketConnection(pushSender, receiptSender, storedMessages,
                                                             account, device, client);

    connection.onDispatchSubscribed(websocketAddress.serialize());
    verify(client, times(3)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(Optional.class));

    assertTrue(futures.size() == 3);

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(1).set(response);

    futures.get(0).setException(new IOException());
    futures.get(2).setException(new IOException());

    verify(storedMessages, times(1)).delete(eq(account.getNumber()), eq(2L));
    verify(receiptSender, times(1)).sendReceipt(eq(account), eq("sender1"), eq(2222L), eq(Optional.<String>absent()));

    connection.onDispatchUnsubscribed(websocketAddress.serialize());
    verify(client).close(anyInt(), anyString());
  }

  @Test
  public void testOnlineSend() throws Exception {
    MessagesManager storedMessages = mock(MessagesManager.class);
    OutgoingMessageSignal firstMessage = OutgoingMessageSignal.newBuilder()
                                                              .setMessage(ByteString.copyFrom("first".getBytes()))
                                                              .setSource("sender1")
                                                              .setTimestamp(System.currentTimeMillis())
                                                              .setSourceDevice(1)
                                                              .setType(OutgoingMessageSignal.Type.CIPHERTEXT_VALUE)
                                                              .build();

    OutgoingMessageSignal secondMessage = OutgoingMessageSignal.newBuilder()
                                                               .setMessage(ByteString.copyFrom("second".getBytes()))
                                                               .setSource("sender2")
                                                               .setTimestamp(System.currentTimeMillis())
                                                               .setSourceDevice(2)
                                                               .setType(OutgoingMessageSignal.Type.CIPHERTEXT_VALUE)
                                                               .build();

    List<OutgoingMessageEntity> pendingMessages = new LinkedList<>();

    when(device.getId()).thenReturn(2L);
    when(device.getSignalingKey()).thenReturn(Base64.encodeBytes(new byte[52]));

    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));
    when(account.getNumber()).thenReturn("+14152222222");

    final Device sender1device = mock(Device.class);

    Set<Device> sender1devices = new HashSet<Device>() {{
      add(sender1device);
    }};

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.get("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.get("sender2")).thenReturn(Optional.<Account>absent());

    when(storedMessages.getMessagesForDevice(account.getNumber(), device.getId()))
        .thenReturn(pendingMessages);

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

    WebsocketAddress websocketAddress = new WebsocketAddress(account.getNumber(), device.getId());
    WebSocketConnection connection = new WebSocketConnection(pushSender, receiptSender, storedMessages,
                                                             account, device, client);

    connection.onDispatchSubscribed(websocketAddress.serialize());
    connection.onDispatchMessage(websocketAddress.serialize(), PubSubProtos.PubSubMessage.newBuilder()
                                                                                         .setType(PubSubProtos.PubSubMessage.Type.DELIVER)
                                                                                         .setContent(ByteString.copyFrom(firstMessage.toByteArray()))
                                                                                         .build().toByteArray());

    connection.onDispatchMessage(websocketAddress.serialize(), PubSubProtos.PubSubMessage.newBuilder()
                                                                                         .setType(PubSubProtos.PubSubMessage.Type.DELIVER)
                                                                                         .setContent(ByteString.copyFrom(secondMessage.toByteArray()))
                                                                                         .build().toByteArray());

    verify(client, times(2)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(Optional.class));

    assertEquals(futures.size(), 2);

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(1).set(response);
    futures.get(0).setException(new IOException());

    verify(receiptSender, times(1)).sendReceipt(eq(account), eq("sender2"), eq(secondMessage.getTimestamp()), eq(Optional.<String>absent()));
    verify(pushSender, times(1)).sendMessage(eq(account), eq(device), any(OutgoingMessageSignal.class));

    connection.onDispatchUnsubscribed(websocketAddress.serialize());
    verify(client).close(anyInt(), anyString());
  }

  private OutgoingMessageEntity createMessage(long id, String sender, long timestamp, boolean receipt, String content) {
    return new OutgoingMessageEntity(id, receipt ? OutgoingMessageSignal.Type.RECEIPT_VALUE : OutgoingMessageSignal.Type.CIPHERTEXT_VALUE,
                                     null, timestamp, sender, 1, content.getBytes());
  }

}
