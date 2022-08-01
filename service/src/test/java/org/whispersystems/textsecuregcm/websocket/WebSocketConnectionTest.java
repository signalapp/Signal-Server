/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.auth.basic.BasicCredentials;
import io.lettuce.core.RedisException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.auth.WebSocketAuthenticator.AuthenticationResult;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.session.WebSocketSessionContext;

class WebSocketConnectionTest {

  private static final String VALID_USER = "+14152222222";
  private static final String INVALID_USER = "+14151111111";

  private static final int SOURCE_DEVICE_ID = 1;

  private static final String VALID_PASSWORD = "secure";
  private static final String INVALID_PASSWORD = "insecure";

  private AccountAuthenticator accountAuthenticator;
  private AccountsManager accountsManager;
  private Account account;
  private Device device;
  private AuthenticatedAccount auth;
  private UpgradeRequest upgradeRequest;
  private ReceiptSender receiptSender;
  private ApnFallbackManager apnFallbackManager;
  private ScheduledExecutorService retrySchedulingExecutor;

  @BeforeEach
  void setup() {
    accountAuthenticator = mock(AccountAuthenticator.class);
    accountsManager = mock(AccountsManager.class);
    account = mock(Account.class);
    device = mock(Device.class);
    auth = new AuthenticatedAccount(() -> new Pair<>(account, device));
    upgradeRequest = mock(UpgradeRequest.class);
    receiptSender = mock(ReceiptSender.class);
    apnFallbackManager = mock(ApnFallbackManager.class);
    retrySchedulingExecutor = mock(ScheduledExecutorService.class);
  }

  @Test
  void testCredentials() {
    MessagesManager storedMessages = mock(MessagesManager.class);
    WebSocketAccountAuthenticator webSocketAuthenticator = new WebSocketAccountAuthenticator(accountAuthenticator);
    AuthenticatedConnectListener connectListener = new AuthenticatedConnectListener(receiptSender, storedMessages,
        mock(MessageSender.class), apnFallbackManager, mock(ClientPresenceManager.class),
        retrySchedulingExecutor);
    WebSocketSessionContext sessionContext = mock(WebSocketSessionContext.class);

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(new AuthenticatedAccount(() -> new Pair<>(account, device))));

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(INVALID_USER, INVALID_PASSWORD))))
        .thenReturn(Optional.empty());


    when(upgradeRequest.getParameterMap()).thenReturn(Map.of(
        "login", List.of(VALID_USER),
        "password", List.of(VALID_PASSWORD)));

    AuthenticationResult<AuthenticatedAccount> account = webSocketAuthenticator.authenticate(upgradeRequest);
    when(sessionContext.getAuthenticated(AuthenticatedAccount.class)).thenReturn(account.getUser().orElse(null));

    connectListener.onWebSocketConnect(sessionContext);

    verify(sessionContext).addListener(any(WebSocketSessionContext.WebSocketEventListener.class));

    when(upgradeRequest.getParameterMap()).thenReturn(Map.of(
        "login", List.of(INVALID_USER),
        "password", List.of(INVALID_PASSWORD)
    ));

    account = webSocketAuthenticator.authenticate(upgradeRequest);
    assertFalse(account.getUser().isPresent());
    assertTrue(account.isRequired());
  }

  @Test
  void testOpen() throws Exception {
    MessagesManager storedMessages = mock(MessagesManager.class);

    UUID accountUuid = UUID.randomUUID();
    UUID senderOneUuid = UUID.randomUUID();
    UUID senderTwoUuid = UUID.randomUUID();

    List<Envelope> outgoingMessages = List.of(createMessage("sender1", senderOneUuid, accountUuid, 1111, "first"),
        createMessage("sender1", senderOneUuid, accountUuid, 2222, "second"),
        createMessage("sender2", senderTwoUuid, accountUuid, 3333, "third"));

    final long deviceId = 2L;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = List.of(sender1device);

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.getByE164("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.getByE164("sender2")).thenReturn(Optional.empty());

    String userAgent = "user-agent";

    when(storedMessages.getMessagesForDevice(account.getUuid(), device.getId(), userAgent, false))
        .thenReturn(new Pair<>(outgoingMessages, false));

    final List<CompletableFuture<WebSocketResponseMessage>> futures = new LinkedList<>();
    final WebSocketClient                                   client  = mock(WebSocketClient.class);

    when(client.getUserAgent()).thenReturn(userAgent);
    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), ArgumentMatchers.nullable(List.class), ArgumentMatchers.<Optional<byte[]>>any()))
        .thenAnswer(new Answer<CompletableFuture<WebSocketResponseMessage>>() {
          @Override
          public CompletableFuture<WebSocketResponseMessage> answer(InvocationOnMock invocationOnMock) {
            CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
            futures.add(future);
            return future;
          }
        });

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages,
        auth, device, client, retrySchedulingExecutor);

    connection.start();
    verify(client, times(3)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class),
        ArgumentMatchers.<Optional<byte[]>>any());

    assertEquals(3, futures.size());

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(1).complete(response);

    futures.get(0).completeExceptionally(new IOException());
    futures.get(2).completeExceptionally(new IOException());

    verify(storedMessages, times(1)).delete(eq(accountUuid), eq(deviceId),
        eq(UUID.fromString(outgoingMessages.get(1).getServerGuid())), eq(outgoingMessages.get(1).getServerTimestamp()));
    verify(receiptSender, times(1)).sendReceipt(eq(accountUuid), eq(deviceId), eq(senderOneUuid),
        eq(2222L));

    connection.stop();
    verify(client).close(anyInt(), anyString());
  }

  @Test
  public void testOnlineSend() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    when(messagesManager.getMessagesForDevice(eq(accountUuid), eq(1L), eq("Test-UA"), anyBoolean()))
        .thenReturn(new Pair<>(Collections.emptyList(), false))
        .thenReturn(new Pair<>(List.of(createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 1111, "first")), false))
        .thenReturn(new Pair<>(List.of(createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 2222, "second")), false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final AtomicInteger sendCounter = new AtomicInteger(0);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class))).thenAnswer((Answer<CompletableFuture<WebSocketResponseMessage>>)invocation -> {
      synchronized (sendCounter) {
        sendCounter.incrementAndGet();
        sendCounter.notifyAll();
      }

      return CompletableFuture.completedFuture(successResponse);
    });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      // This is a little hacky and non-obvious, but because the first call to getMessagesForDevice returns empty list of
      // messages, the call to CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded
      // future, and the whenComplete method will get called immediately on THIS thread, so we don't need to synchronize
      // or wait for anything.
      connection.start();

      connection.handleNewMessagesAvailable();

      synchronized (sendCounter) {
        while (sendCounter.get() < 1) {
          sendCounter.wait();
        }
      }

      connection.handleNewMessagesAvailable();

      synchronized (sendCounter) {
        while (sendCounter.get() < 2) {
          sendCounter.wait();
        }
      }
    });

    verify(client, times(1)).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
    verify(client, times(2)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class));
  }

  @Test
  void testPendingSend() throws Exception {
    MessagesManager storedMessages  = mock(MessagesManager.class);

    final UUID accountUuid = UUID.randomUUID();
    final UUID senderTwoUuid = UUID.randomUUID();

    final Envelope firstMessage = Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setSourceUuid(UUID.randomUUID().toString())
        .setDestinationUuid(accountUuid.toString())
        .setUpdatedPni(UUID.randomUUID().toString())
        .setTimestamp(System.currentTimeMillis())
        .setSourceDevice(1)
        .setType(Envelope.Type.CIPHERTEXT)
        .build();

    final Envelope secondMessage = Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setSourceUuid(senderTwoUuid.toString())
        .setDestinationUuid(accountUuid.toString())
        .setTimestamp(System.currentTimeMillis())
        .setSourceDevice(2)
        .setType(Envelope.Type.CIPHERTEXT)
        .build();

    final List<Envelope> pendingMessages = List.of(firstMessage, secondMessage);

    final long deviceId = 2L;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = List.of(sender1device);

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.getByE164("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.getByE164("sender2")).thenReturn(Optional.<Account>empty());

    String userAgent = "user-agent";

    when(storedMessages.getMessagesForDevice(account.getUuid(), device.getId(), userAgent, false))
        .thenReturn(new Pair<>(pendingMessages, false));

    final List<CompletableFuture<WebSocketResponseMessage>> futures = new LinkedList<>();
    final WebSocketClient                                   client  = mock(WebSocketClient.class);

    when(client.getUserAgent()).thenReturn(userAgent);
    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any()))
        .thenAnswer((Answer<CompletableFuture<WebSocketResponseMessage>>) invocationOnMock -> {
          CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
          futures.add(future);
          return future;
        });

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages,
        auth, device, client, retrySchedulingExecutor);

    connection.start();

    verify(client, times(2)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any());

    assertEquals(futures.size(), 2);

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(1).complete(response);
    futures.get(0).completeExceptionally(new IOException());

    verify(receiptSender, times(1)).sendReceipt(eq(account.getUuid()), eq(deviceId), eq(senderTwoUuid),
        eq(secondMessage.getTimestamp()));

    connection.stop();
    verify(client).close(anyInt(), anyString());
  }

  @Test
  void testProcessStoredMessageConcurrency() throws InterruptedException {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    final AtomicBoolean threadWaiting = new AtomicBoolean(false);
    final AtomicBoolean returnMessageList = new AtomicBoolean(false);

    when(messagesManager.getMessagesForDevice(account.getUuid(), 1L, client.getUserAgent(), false)).thenAnswer(
        (Answer<OutgoingMessageEntityList>) invocation -> {
      synchronized (threadWaiting) {
        threadWaiting.set(true);
        threadWaiting.notifyAll();
      }

      synchronized (returnMessageList) {
        while (!returnMessageList.get()) {
          returnMessageList.wait();
        }
      }

      return new OutgoingMessageEntityList(Collections.emptyList(), false);
    });

    final Thread[]       threads               = new Thread[10];
    final CountDownLatch unblockedThreadsLatch = new CountDownLatch(threads.length - 1);


    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread(() -> {
          connection.processStoredMessages();
          unblockedThreadsLatch.countDown();
        });

        threads[i].start();
      }

      unblockedThreadsLatch.await();

      synchronized (threadWaiting) {
        while (!threadWaiting.get()) {
          threadWaiting.wait();
        }
      }

      synchronized (returnMessageList) {
        returnMessageList.set(true);
        returnMessageList.notifyAll();
      }

      for (final Thread thread : threads) {
        thread.join();
      }
    });

    verify(messagesManager).getMessagesForDevice(any(UUID.class), anyLong(), anyString(), eq(false));
  }

  @Test
  void testProcessStoredMessagesMultiplePages() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    final List<Envelope> firstPageMessages =
        List.of(createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 1111, "first"),
            createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 2222, "second"));

    final List<Envelope> secondPageMessages =
            List.of(createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 3333, "third"));

    when(messagesManager.getMessagesForDevice(account.getUuid(), 1L, client.getUserAgent(), false))
            .thenReturn(new Pair<>(firstPageMessages, true))
            .thenReturn(new Pair<>(secondPageMessages, false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch sendLatch = new CountDownLatch(firstPageMessages.size() + secondPageMessages.size());

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class))).thenAnswer((Answer<CompletableFuture<WebSocketResponseMessage>>)invocation -> {
      sendLatch.countDown();
      return CompletableFuture.completedFuture(successResponse);
    });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      connection.processStoredMessages();

      sendLatch.await();
    });

    verify(client, times(firstPageMessages.size() + secondPageMessages.size())).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class));
    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  @Test
  void testProcessStoredMessagesContainsSenderUuid() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    final UUID senderUuid = UUID.randomUUID();
    final List<Envelope> messages = List.of(
        createMessage("senderE164", senderUuid, UUID.randomUUID(), 1111L, "message the first"));

    when(messagesManager.getMessagesForDevice(account.getUuid(), 1L, client.getUserAgent(), false))
        .thenReturn(new Pair<>(messages, false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch sendLatch = new CountDownLatch(messages.size());

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class))).thenAnswer(invocation -> {
      sendLatch.countDown();
      return CompletableFuture.completedFuture(successResponse);
    });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      connection.processStoredMessages();

      sendLatch.await();
    });

    verify(client, times(messages.size())).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), argThat(argument -> {
      if (argument.isEmpty()) {
        return false;
      }

      final byte[] body = argument.get();
      try {
        final Envelope envelope = Envelope.parseFrom(body);
        if (!envelope.hasSourceUuid() || envelope.getSourceUuid().length() == 0) {
          return false;
        }
        return envelope.getSourceUuid().equals(senderUuid.toString());
      } catch (InvalidProtocolBufferException e) {
        return false;
      }
    }));
    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  @Test
  void testProcessStoredMessagesSingleEmptyCall() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    when(messagesManager.getMessagesForDevice(eq(accountUuid), eq(1L), eq("Test-UA"), anyBoolean()))
        .thenReturn(new Pair<>(Collections.emptyList(), false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    // This is a little hacky and non-obvious, but because we're always returning an empty list of messages, the call to
    // CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded future, and the
    // whenComplete method will get called immediately on THIS thread, so we don't need to synchronize or wait for
    // anything.
    connection.processStoredMessages();
    connection.processStoredMessages();

    verify(client, times(1)).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  @Test
  public void testRequeryOnStateMismatch() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);
    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    final List<Envelope> firstPageMessages =
        List.of(createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 1111, "first"),
            createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 2222, "second"));

    final List<Envelope> secondPageMessages =
            List.of(createMessage("sender1", UUID.randomUUID(), UUID.randomUUID(), 3333, "third"));

    when(messagesManager.getMessagesForDevice(eq(accountUuid), eq(1L), eq("Test-UA"), anyBoolean()))
            .thenReturn(new Pair<>(firstPageMessages, false))
            .thenReturn(new Pair<>(secondPageMessages, false))
            .thenReturn(new Pair<>(Collections.emptyList(), false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch sendLatch = new CountDownLatch(firstPageMessages.size() + secondPageMessages.size());

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class))).thenAnswer((Answer<CompletableFuture<WebSocketResponseMessage>>)invocation -> {
      connection.handleNewMessagesAvailable();
      sendLatch.countDown();

      return CompletableFuture.completedFuture(successResponse);
    });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      connection.processStoredMessages();

      sendLatch.await();
    });

    verify(client, times(firstPageMessages.size() + secondPageMessages.size())).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class));
    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  @Test
  void testProcessCachedMessagesOnly() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    when(messagesManager.getMessagesForDevice(eq(accountUuid), eq(1L), eq("Test-UA"), anyBoolean()))
        .thenReturn(new Pair<>(Collections.emptyList(), false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    // This is a little hacky and non-obvious, but because we're always returning an empty list of messages, the call to
    // CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded future, and the
    // whenComplete method will get called immediately on THIS thread, so we don't need to synchronize or wait for
    // anything.
    connection.processStoredMessages();

    verify(messagesManager).getMessagesForDevice(account.getUuid(), device.getId(), client.getUserAgent(), false);

    connection.handleNewMessagesAvailable();

    verify(messagesManager).getMessagesForDevice(account.getUuid(), device.getId(), client.getUserAgent(), true);
  }

  @Test
  void testProcessDatabaseMessagesAfterPersist() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);
    when(client.getUserAgent()).thenReturn("Test-UA");

    when(messagesManager.getMessagesForDevice(eq(accountUuid), eq(1L), eq("Test-UA"), anyBoolean()))
        .thenReturn(new Pair<>(Collections.emptyList(), false));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    // This is a little hacky and non-obvious, but because we're always returning an empty list of messages, the call to
    // CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded future, and the
    // whenComplete method will get called immediately on THIS thread, so we don't need to synchronize or wait for
    // anything.
    connection.processStoredMessages();
    connection.handleMessagesPersisted();

    verify(messagesManager, times(2)).getMessagesForDevice(account.getUuid(), device.getId(), client.getUserAgent(), false);
  }

  @Test
  void testDiscardOversizedMessagesForDesktop() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    UUID accountUuid   = UUID.randomUUID();
    UUID senderOneUuid = UUID.randomUUID();
    UUID senderTwoUuid = UUID.randomUUID();

    List<Envelope> outgoingMessages = List.of(
        createMessage("sender1", senderOneUuid, UUID.randomUUID(), 1111, "first"),
        createMessage("sender1", senderOneUuid, UUID.randomUUID(), 2222,
            RandomStringUtils.randomAlphanumeric(WebSocketConnection.MAX_DESKTOP_MESSAGE_SIZE + 1)),
        createMessage("sender2", senderTwoUuid, UUID.randomUUID(), 3333, "third"));

    when(device.getId()).thenReturn(2L);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = List.of(sender1device);

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.getByE164("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.getByE164("sender2")).thenReturn(Optional.empty());

    String userAgent = "Signal-Desktop/1.2.3";

    when(storedMessages.getMessagesForDevice(account.getUuid(), device.getId(), userAgent, false))
            .thenReturn(new Pair<>(outgoingMessages, false));

    final List<CompletableFuture<WebSocketResponseMessage>> futures = new LinkedList<>();
    final WebSocketClient                                   client  = mock(WebSocketClient.class);

    when(client.getUserAgent()).thenReturn(userAgent);
    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), ArgumentMatchers.nullable(List.class),
        ArgumentMatchers.<Optional<byte[]>>any()))
        .thenAnswer(new Answer<CompletableFuture<WebSocketResponseMessage>>() {
          @Override
          public CompletableFuture<WebSocketResponseMessage> answer(InvocationOnMock invocationOnMock) {
            CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
            futures.add(future);
            return future;
          }
        });

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor);

    connection.start();
    verify(client, times(2)).sendRequest(eq("PUT"), eq("/api/v1/message"), ArgumentMatchers.nullable(List.class),
        ArgumentMatchers.<Optional<byte[]>>any());

    assertEquals(2, futures.size());

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(0).complete(response);
    futures.get(1).complete(response);

    // We should delete all three messages even though we only sent two; one got discarded because it was too big for
    // desktop clients.
    verify(storedMessages, times(3)).delete(eq(accountUuid), eq(2L), any(UUID.class), any(Long.class));

    connection.stop();
    verify(client).close(anyInt(), anyString());
  }

  @Test
  void testSendOversizedMessagesForNonDesktop() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    UUID accountUuid   = UUID.randomUUID();
    UUID senderOneUuid = UUID.randomUUID();
    UUID senderTwoUuid = UUID.randomUUID();

    List<Envelope> outgoingMessages = List.of(createMessage("sender1", senderOneUuid, UUID.randomUUID(), 1111, "first"),
        createMessage("sender1", senderOneUuid, UUID.randomUUID(), 2222,
            RandomStringUtils.randomAlphanumeric(WebSocketConnection.MAX_DESKTOP_MESSAGE_SIZE + 1)),
        createMessage("sender2", senderTwoUuid, UUID.randomUUID(), 3333, "third"));

    when(device.getId()).thenReturn(2L);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = List.of(sender1device);

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.getByE164("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.getByE164("sender2")).thenReturn(Optional.empty());

    String userAgent = "Signal-Android/4.68.3";

    when(storedMessages.getMessagesForDevice(account.getUuid(), device.getId(), userAgent, false))
            .thenReturn(new Pair<>(outgoingMessages, false));

    final List<CompletableFuture<WebSocketResponseMessage>> futures = new LinkedList<>();
    final WebSocketClient                                   client  = mock(WebSocketClient.class);

    when(client.getUserAgent()).thenReturn(userAgent);
    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), ArgumentMatchers.nullable(List.class),
        ArgumentMatchers.<Optional<byte[]>>any()))
        .thenAnswer(new Answer<CompletableFuture<WebSocketResponseMessage>>() {
          @Override
          public CompletableFuture<WebSocketResponseMessage> answer(InvocationOnMock invocationOnMock) {
            CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
            futures.add(future);
            return future;
          }
        });

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor);

    connection.start();
    verify(client, times(3)).sendRequest(eq("PUT"), eq("/api/v1/message"), ArgumentMatchers.nullable(List.class),
        ArgumentMatchers.<Optional<byte[]>>any());

    assertEquals(3, futures.size());

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(0).complete(response);
    futures.get(1).complete(response);
    futures.get(2).complete(response);

    verify(storedMessages, times(3)).delete(eq(accountUuid), eq(2L), any(UUID.class), any(Long.class));

    connection.stop();
    verify(client).close(anyInt(), anyString());
  }

  @Test
  void testRetrieveMessageException() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    UUID accountUuid = UUID.randomUUID();

    when(device.getId()).thenReturn(2L);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    String userAgent = "Signal-Android/4.68.3";

    when(storedMessages.getMessagesForDevice(account.getUuid(), device.getId(), userAgent, false))
        .thenThrow(new RedisException("OH NO"));

    when(retrySchedulingExecutor.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(
        (Answer<ScheduledFuture<?>>) invocation -> {
          invocation.getArgument(0, Runnable.class).run();
          return mock(ScheduledFuture.class);
        });

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor);
    connection.start();

    verify(retrySchedulingExecutor, times(WebSocketConnection.MAX_CONSECUTIVE_RETRIES)).schedule(any(Runnable.class),
        anyLong(), any());
    verify(client).close(eq(1011), anyString());
  }

  @Test
  void testRetrieveMessageExceptionClientDisconnected() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    UUID accountUuid = UUID.randomUUID();

    when(device.getId()).thenReturn(2L);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    String userAgent = "Signal-Android/4.68.3";

    when(storedMessages.getMessagesForDevice(account.getUuid(), device.getId(), userAgent, false))
        .thenThrow(new RedisException("OH NO"));

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(false);

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor);
    connection.start();

    verify(retrySchedulingExecutor, never()).schedule(any(Runnable.class), anyLong(), any());
    verify(client, never()).close(anyInt(), anyString());
  }

  private Envelope createMessage(String sender, UUID senderUuid, UUID destinationUuid, long timestamp, String content) {
    return Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setType(Envelope.Type.CIPHERTEXT)
        .setTimestamp(timestamp)
        .setServerTimestamp(0)
        .setSourceUuid(senderUuid.toString())
        .setSourceDevice(SOURCE_DEVICE_ID)
        .setDestinationUuid(destinationUuid.toString())
        .setContent(ByteString.copyFrom(content.getBytes(StandardCharsets.UTF_8)))
        .build();
  }

}
