/*
 * Copyright 2013-2022 Signal Messenger, LLC
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
import static org.mockito.ArgumentMatchers.nullable;
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

import com.google.common.net.HttpHeaders;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.auth.basic.BasicCredentials;
import io.lettuce.core.RedisException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

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
    retrySchedulingExecutor = mock(ScheduledExecutorService.class);
  }

  @AfterEach
  void teardown() {
    StepVerifier.resetDefaultTimeout();
  }

  @Test
  void testCredentials() {
    MessagesManager storedMessages = mock(MessagesManager.class);
    WebSocketAccountAuthenticator webSocketAuthenticator = new WebSocketAccountAuthenticator(accountAuthenticator);
    AuthenticatedConnectListener connectListener = new AuthenticatedConnectListener(receiptSender, storedMessages,
        mock(PushNotificationManager.class), mock(ClientPresenceManager.class),
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
  void testOpen() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    UUID accountUuid = UUID.randomUUID();
    UUID senderOneUuid = UUID.randomUUID();
    UUID senderTwoUuid = UUID.randomUUID();

    List<Envelope> outgoingMessages = List.of(createMessage(senderOneUuid, accountUuid, 1111, "first"),
        createMessage(senderOneUuid, accountUuid, 2222, "second"),
        createMessage(senderTwoUuid, accountUuid, 3333, "third"));

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

    String userAgent = HttpHeaders.USER_AGENT;

    when(storedMessages.getMessagesForDeviceReactive(account.getUuid(), device.getId(), false))
        .thenReturn(Flux.fromIterable(outgoingMessages));

    final List<CompletableFuture<WebSocketResponseMessage>> futures = new LinkedList<>();
    final WebSocketClient client = mock(WebSocketClient.class);

    when(client.getUserAgent()).thenReturn(userAgent);
    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), nullable(List.class), any()))
        .thenAnswer(invocation -> {
          CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
          futures.add(future);
          return future;
        });

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages,
        auth, device, client, retrySchedulingExecutor, Schedulers.immediate());

    connection.start();
    verify(client, times(3)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class),
        any());

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
        retrySchedulingExecutor, Schedulers.immediate());

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(1L), anyBoolean()))
        .thenReturn(Flux.empty())
        .thenReturn(Flux.just(createMessage(UUID.randomUUID(), UUID.randomUUID(), 1111, "first")))
        .thenReturn(Flux.just(createMessage(UUID.randomUUID(), UUID.randomUUID(), 2222, "second")))
        .thenReturn(Flux.empty());

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final AtomicInteger sendCounter = new AtomicInteger(0);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class)))
        .thenAnswer(invocation -> {
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
  void testPendingSend() {
    MessagesManager storedMessages = mock(MessagesManager.class);

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
    when(accountsManager.getByE164("sender2")).thenReturn(Optional.empty());

    String userAgent = HttpHeaders.USER_AGENT;

    when(storedMessages.getMessagesForDeviceReactive(account.getUuid(), device.getId(), false))
        .thenReturn(Flux.fromIterable(pendingMessages));

    final List<CompletableFuture<WebSocketResponseMessage>> futures = new LinkedList<>();
    final WebSocketClient client = mock(WebSocketClient.class);

    when(client.getUserAgent()).thenReturn(userAgent);
    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any()))
        .thenAnswer((Answer<CompletableFuture<WebSocketResponseMessage>>) invocationOnMock -> {
          CompletableFuture<WebSocketResponseMessage> future = new CompletableFuture<>();
          futures.add(future);
          return future;
        });

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages,
        auth, device, client, retrySchedulingExecutor, Schedulers.immediate());

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
  void testProcessStoredMessageConcurrency() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    final AtomicBoolean threadWaiting = new AtomicBoolean(false);
    final AtomicBoolean returnMessageList = new AtomicBoolean(false);

    when(
        messagesManager.getMessagesForDeviceReactive(account.getUuid(), 1L, false))
        .thenAnswer(invocation -> {
          synchronized (threadWaiting) {
            threadWaiting.set(true);
            threadWaiting.notifyAll();
          }

          synchronized (returnMessageList) {
            while (!returnMessageList.get()) {
              returnMessageList.wait();
            }
          }

          return Flux.empty();
        });

    final Thread[] threads = new Thread[10];
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

    verify(messagesManager).getMessagesForDeviceReactive(any(UUID.class), anyLong(), eq(false));
  }

  @Test
  void testProcessStoredMessagesMultiplePages() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());

    when(account.getNumber()).thenReturn("+18005551234");
    final UUID accountUuid = UUID.randomUUID();
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    final List<Envelope> firstPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 1111, "first"),
            createMessage(UUID.randomUUID(), UUID.randomUUID(), 2222, "second"));

    final List<Envelope> secondPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 3333, "third"));

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(1L), eq(false)))
        .thenReturn(Flux.fromStream(Stream.concat(firstPageMessages.stream(), secondPageMessages.stream())));

    when(messagesManager.delete(eq(accountUuid), eq(1L), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch queueEmptyLatch = new CountDownLatch(1);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class)))
        .thenAnswer(invocation -> {
          return CompletableFuture.completedFuture(successResponse);
        });

    when(client.sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty())))
        .thenAnswer(invocation -> {
          queueEmptyLatch.countDown();
          return CompletableFuture.completedFuture(successResponse);
        });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      connection.processStoredMessages();

      queueEmptyLatch.await();
    });

    verify(client, times(firstPageMessages.size() + secondPageMessages.size())).sendRequest(eq("PUT"),
        eq("/api/v1/message"), any(List.class), any(Optional.class));
    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  @Test
  void testProcessStoredMessagesContainsSenderUuid() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());

    when(account.getNumber()).thenReturn("+18005551234");
    final UUID accountUuid = UUID.randomUUID();
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    final UUID senderUuid = UUID.randomUUID();
    final List<Envelope> messages = List.of(
        createMessage(senderUuid, UUID.randomUUID(), 1111L, "message the first"));

    when(messagesManager.getMessagesForDeviceReactive(account.getUuid(), 1L, false))
        .thenReturn(Flux.fromIterable(messages))
        .thenReturn(Flux.empty());

    when(messagesManager.delete(eq(accountUuid), eq(1L), any(UUID.class), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch queueEmptyLatch = new CountDownLatch(1);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class))).thenAnswer(
        invocation -> CompletableFuture.completedFuture(successResponse));

    when(client.sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty())))
        .thenAnswer(invocation -> {
          queueEmptyLatch.countDown();
          return CompletableFuture.completedFuture(successResponse);
        });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      connection.processStoredMessages();
      queueEmptyLatch.await();
    });

    verify(client, times(messages.size())).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class),
        argThat(argument -> {
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
        retrySchedulingExecutor, Schedulers.immediate());

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(1L), anyBoolean()))
        .thenReturn(Flux.empty());

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
        retrySchedulingExecutor, Schedulers.immediate());
    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    final List<Envelope> firstPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 1111, "first"),
            createMessage(UUID.randomUUID(), UUID.randomUUID(), 2222, "second"));

    final List<Envelope> secondPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 3333, "third"));

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(1L), anyBoolean()))
        .thenReturn(Flux.fromIterable(firstPageMessages))
        .thenReturn(Flux.fromIterable(secondPageMessages))
        .thenReturn(Flux.empty());

    when(messagesManager.delete(eq(accountUuid), eq(1L), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch queueEmptyLatch = new CountDownLatch(1);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class)))
        .thenAnswer(invocation -> {
          connection.handleNewMessagesAvailable();

          return CompletableFuture.completedFuture(successResponse);
        });

    when(client.sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty())))
        .thenAnswer(invocation -> {
          queueEmptyLatch.countDown();
          return CompletableFuture.completedFuture(successResponse);
        });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      connection.processStoredMessages();

      queueEmptyLatch.await();
    });

    verify(client, times(firstPageMessages.size() + secondPageMessages.size())).sendRequest(eq("PUT"),
        eq("/api/v1/message"), any(List.class), any(Optional.class));
    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  @Test
  void testProcessCachedMessagesOnly() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(1L), anyBoolean()))
        .thenReturn(Flux.empty());

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    // This is a little hacky and non-obvious, but because we're always returning an empty list of messages, the call to
    // CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded future, and the
    // whenComplete method will get called immediately on THIS thread, so we don't need to synchronize or wait for
    // anything.
    connection.processStoredMessages();

    verify(messagesManager).getMessagesForDeviceReactive(account.getUuid(), device.getId(), false);

    connection.handleNewMessagesAvailable();

    verify(messagesManager).getMessagesForDeviceReactive(account.getUuid(), device.getId(), true);
  }

  @Test
  void testProcessDatabaseMessagesAfterPersist() {
    final MessagesManager messagesManager = mock(MessagesManager.class);
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = new WebSocketConnection(receiptSender, messagesManager, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(accountUuid);
    when(device.getId()).thenReturn(1L);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(1L), anyBoolean()))
        .thenReturn(Flux.empty());

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    // This is a little hacky and non-obvious, but because we're always returning an empty list of messages, the call to
    // CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded future, and the
    // whenComplete method will get called immediately on THIS thread, so we don't need to synchronize or wait for
    // anything.
    connection.processStoredMessages();
    connection.handleMessagesPersisted();

    verify(messagesManager, times(2)).getMessagesForDeviceReactive(account.getUuid(), device.getId(), false);
  }

  @Test
  void testRetrieveMessageException() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    UUID accountUuid = UUID.randomUUID();

    when(device.getId()).thenReturn(2L);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    when(storedMessages.getMessagesForDeviceReactive(account.getUuid(), device.getId(), false))
        .thenReturn(Flux.error(new RedisException("OH NO")));

    when(retrySchedulingExecutor.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(
        (Answer<ScheduledFuture<?>>) invocation -> {
          invocation.getArgument(0, Runnable.class).run();
          return mock(ScheduledFuture.class);
        });

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());
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

    when(storedMessages.getMessagesForDeviceReactive(account.getUuid(), device.getId(), false))
        .thenReturn(Flux.error(new RedisException("OH NO")));

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(false);

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());
    connection.start();

    verify(retrySchedulingExecutor, never()).schedule(any(Runnable.class), anyLong(), any());
    verify(client, never()).close(anyInt(), anyString());
  }

  @Test
  @Disabled("This test is flaky")
  void testReactivePublisherLimitRate() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    final UUID accountUuid = UUID.randomUUID();

    final long deviceId = 2L;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    final int totalMessages = 10;
    final AtomicReference<FluxSink<Envelope>> sink = new AtomicReference<>();

    final AtomicLong maxRequest = new AtomicLong(-1);
    final Flux<Envelope> flux = Flux.create(s -> {
      sink.set(s);
      s.onRequest(n -> {
        if (maxRequest.get() < n) {
          maxRequest.set(n);
        }
      });
    });

    when(storedMessages.getMessagesForDeviceReactive(eq(accountUuid), eq(deviceId), anyBoolean()))
        .thenReturn(flux);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);
    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);
    when(client.sendRequest(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));
    when(storedMessages.delete(any(), anyLong(), any(), any())).thenReturn(
        CompletableFuture.completedFuture(Optional.empty()));

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor);

    connection.start();

    StepVerifier.setDefaultTimeout(Duration.ofSeconds(5));

    StepVerifier.create(flux, 0)
        .expectSubscription()
        .thenRequest(totalMessages * 2)
        .then(() -> {
          for (long i = 0; i < totalMessages; i++) {
            sink.get().next(createMessage(UUID.randomUUID(), accountUuid, 1111 * i + 1, "message " + i));
          }
          sink.get().complete();
        })
        .expectNextCount(totalMessages)
        .expectComplete()
        .log()
        .verify();

    assertEquals(WebSocketConnection.MESSAGE_PUBLISHER_LIMIT_RATE, maxRequest.get());
  }

  @Test
  void testReactivePublisherDisposedWhenConnectionStopped() {
    MessagesManager storedMessages = mock(MessagesManager.class);

    final UUID accountUuid = UUID.randomUUID();

    final long deviceId = 2L;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getUuid()).thenReturn(accountUuid);

    final AtomicBoolean canceled = new AtomicBoolean();

    final Flux<Envelope> flux = Flux.create(s -> {
      s.onRequest(n -> {
        // the subscriber should request more than 1 message, but we will only send one, so that
        // we are sure the subscriber is waiting for more when we stop the connection
        assert n > 1;
        s.next(createMessage(UUID.randomUUID(), UUID.randomUUID(), 1111, "first"));
      });

      s.onCancel(() -> canceled.set(true));
    });
    when(storedMessages.getMessagesForDeviceReactive(eq(accountUuid), eq(deviceId), anyBoolean()))
        .thenReturn(flux);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);
    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);
    when(client.sendRequest(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));
    when(storedMessages.delete(any(), anyLong(), any(), any())).thenReturn(
        CompletableFuture.completedFuture(Optional.empty()));

    WebSocketConnection connection = new WebSocketConnection(receiptSender, storedMessages, auth, device, client,
        retrySchedulingExecutor, Schedulers.immediate());

    connection.start();

    verify(client).sendRequest(any(), any(), any(), any());

    // close the connection before the publisher completes
    connection.stop();

    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));

    StepVerifier.create(flux)
        .expectSubscription()
        .expectNextCount(1)
        .then(() -> assertTrue(canceled.get()))
        // this is not entirely intuitive, but expecting a timeout is the recommendation for verifying cancellation
        .expectTimeout(Duration.ofMillis(100))
        .log()
        .verify();
  }

  private Envelope createMessage(UUID senderUuid, UUID destinationUuid, long timestamp, String content) {
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
