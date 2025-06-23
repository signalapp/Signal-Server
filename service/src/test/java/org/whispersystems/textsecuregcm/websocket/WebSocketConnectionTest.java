/*
 * Copyright 2013 Signal Messenger, LLC
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
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.push.WebSocketConnectionEventManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

class WebSocketConnectionTest {

  private static final String VALID_E164 = "+14152222222";
  private static final UUID VALID_UUID = UUID.randomUUID();

  private static final int SOURCE_DEVICE_ID = 1;

  private static final String VALID_PASSWORD = "secure";

  private AccountAuthenticator accountAuthenticator;
  private AccountsManager accountsManager;
  private Account account;
  private Device device;
  private UpgradeRequest upgradeRequest;
  private MessagesManager messagesManager;
  private ReceiptSender receiptSender;
  private ScheduledExecutorService retrySchedulingExecutor;
  private Scheduler messageDeliveryScheduler;
  private ClientReleaseManager clientReleaseManager;

  @BeforeEach
  void setup() {
    accountAuthenticator = mock(AccountAuthenticator.class);
    accountsManager = mock(AccountsManager.class);
    account = mock(Account.class);
    device = mock(Device.class);
    upgradeRequest = mock(UpgradeRequest.class);
    messagesManager = mock(MessagesManager.class);
    receiptSender = mock(ReceiptSender.class);
    retrySchedulingExecutor = mock(ScheduledExecutorService.class);
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
    clientReleaseManager = mock(ClientReleaseManager.class);
  }

  @AfterEach
  void teardown() {
    StepVerifier.resetDefaultTimeout();
    messageDeliveryScheduler.dispose();
  }

  @Test
  void testCredentials() throws Exception {
    WebSocketAccountAuthenticator webSocketAuthenticator =
        new WebSocketAccountAuthenticator(accountAuthenticator);
    AuthenticatedConnectListener connectListener = new AuthenticatedConnectListener(accountsManager, receiptSender, messagesManager,
        new MessageMetrics(), mock(PushNotificationManager.class), mock(PushNotificationScheduler.class),
        mock(WebSocketConnectionEventManager.class), retrySchedulingExecutor,
        messageDeliveryScheduler, clientReleaseManager, mock(MessageDeliveryLoopMonitor.class),
        mock(ExperimentEnrollmentManager.class));
    WebSocketSessionContext sessionContext = mock(WebSocketSessionContext.class);

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_E164, VALID_PASSWORD))))
        .thenReturn(Optional.of(new AuthenticatedDevice(VALID_UUID, Device.PRIMARY_ID, Instant.now())));

    Optional<AuthenticatedDevice> account = webSocketAuthenticator.authenticate(upgradeRequest);
    when(sessionContext.getAuthenticated()).thenReturn(account.orElse(null));
    when(sessionContext.getAuthenticated(AuthenticatedDevice.class)).thenReturn(account.orElse(null));

    final WebSocketClient webSocketClient = mock(WebSocketClient.class);
    when(webSocketClient.getUserAgent()).thenReturn("Signal-Android/6.22.8");
    when(sessionContext.getClient()).thenReturn(webSocketClient);

    // authenticated - valid user
    connectListener.onWebSocketConnect(sessionContext);

    verify(sessionContext, times(1)).addWebsocketClosedListener(
        any(WebSocketSessionContext.WebSocketEventListener.class));

    // unauthenticated
    when(upgradeRequest.getParameterMap()).thenReturn(Map.of());
    account = webSocketAuthenticator.authenticate(upgradeRequest);
    assertFalse(account.isPresent());

    connectListener.onWebSocketConnect(sessionContext);
    verify(sessionContext, times(2)).addWebsocketClosedListener(
        any(WebSocketSessionContext.WebSocketEventListener.class));

    verifyNoMoreInteractions(messagesManager);
  }

  @Test
  void testOpen() {

    UUID accountUuid = UUID.randomUUID();
    UUID senderOneUuid = UUID.randomUUID();
    UUID senderTwoUuid = UUID.randomUUID();

    List<Envelope> outgoingMessages = List.of(createMessage(senderOneUuid, accountUuid, 1111, "first"),
        createMessage(senderOneUuid, accountUuid, 2222, "second"),
        createMessage(senderTwoUuid, accountUuid, 3333, "third"));

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = List.of(sender1device);

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.getByE164("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.getByE164("sender2")).thenReturn(Optional.empty());

    when(messagesManager.delete(any(), any(), any(), any())).thenReturn(
        CompletableFuture.completedFuture(Optional.empty()));

    String userAgent = HttpHeaders.USER_AGENT;

    when(messagesManager.getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false))
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

    WebSocketConnection connection = webSocketConnection(client);

    connection.start();
    verify(client, times(3)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class),
        any());

    assertEquals(3, futures.size());

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(1).complete(response);

    futures.get(0).completeExceptionally(new IOException());
    futures.get(2).completeExceptionally(new IOException());

    verify(messagesManager, times(1)).delete(eq(accountUuid), eq(device),
        eq(UUID.fromString(outgoingMessages.get(1).getServerGuid())), eq(outgoingMessages.get(1).getServerTimestamp()));
    verify(receiptSender, times(1)).sendReceipt(eq(new AciServiceIdentifier(accountUuid)), eq(deviceId), eq(new AciServiceIdentifier(senderOneUuid)),
        eq(2222L));

    connection.stop();
    verify(client).close(anyInt(), anyString());
  }

  @Test
  public void testOnlineSend() {
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), argThat(d -> d.getId() == Device.PRIMARY_ID), anyBoolean()))
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

      connection.handleNewMessageAvailable();

      synchronized (sendCounter) {
        while (sendCounter.get() < 1) {
          sendCounter.wait();
        }
      }

      connection.handleNewMessageAvailable();

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
    final UUID accountUuid = UUID.randomUUID();
    final UUID senderTwoUuid = UUID.randomUUID();

    final Envelope firstMessage = Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setSourceServiceId(UUID.randomUUID().toString())
        .setDestinationServiceId(accountUuid.toString())
        .setUpdatedPni(UUID.randomUUID().toString())
        .setClientTimestamp(System.currentTimeMillis())
        .setSourceDevice(1)
        .setType(Envelope.Type.CIPHERTEXT)
        .build();

    final Envelope secondMessage = Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setSourceServiceId(senderTwoUuid.toString())
        .setDestinationServiceId(accountUuid.toString())
        .setClientTimestamp(System.currentTimeMillis())
        .setSourceDevice(2)
        .setType(Envelope.Type.CIPHERTEXT)
        .build();

    final List<Envelope> pendingMessages = List.of(firstMessage, secondMessage);

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    final Device sender1device = mock(Device.class);

    List<Device> sender1devices = List.of(sender1device);

    Account sender1 = mock(Account.class);
    when(sender1.getDevices()).thenReturn(sender1devices);

    when(accountsManager.getByE164("sender1")).thenReturn(Optional.of(sender1));
    when(accountsManager.getByE164("sender2")).thenReturn(Optional.empty());

    when(messagesManager.delete(any(), any(), any(), any())).thenReturn(
        CompletableFuture.completedFuture(Optional.empty()));

    String userAgent = HttpHeaders.USER_AGENT;

    when(messagesManager.getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false))
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

    WebSocketConnection connection = webSocketConnection(client);

    connection.start();

    verify(client, times(2)).sendRequest(eq("PUT"), eq("/api/v1/message"), any(), any());

    assertEquals(futures.size(), 2);

    WebSocketResponseMessage response = mock(WebSocketResponseMessage.class);
    when(response.getStatus()).thenReturn(200);
    futures.get(1).complete(response);
    futures.get(0).completeExceptionally(new IOException());

    verify(receiptSender, times(1)).sendReceipt(eq(new AciServiceIdentifier(account.getIdentifier(IdentityType.ACI))), eq(deviceId), eq(new AciServiceIdentifier(senderTwoUuid)),
        eq(secondMessage.getClientTimestamp()));

    connection.stop();
    verify(client).close(anyInt(), anyString());
  }

  @Test
  void testProcessStoredMessageConcurrency() {
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    final AtomicBoolean threadWaiting = new AtomicBoolean(false);
    final AtomicBoolean returnMessageList = new AtomicBoolean(false);

    when(
        messagesManager.getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false))
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

    verify(messagesManager).getMessagesForDeviceReactive(any(UUID.class), any(), eq(false));
  }

  @Test
  void testProcessStoredMessagesMultiplePages() {
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    when(account.getNumber()).thenReturn("+18005551234");
    final UUID accountUuid = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    final List<Envelope> firstPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 1111, "first"),
            createMessage(UUID.randomUUID(), UUID.randomUUID(), 2222, "second"));

    final List<Envelope> secondPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 3333, "third"));

    when(messagesManager.getMessagesForDeviceReactive(accountUuid, device, false))
        .thenReturn(Flux.fromStream(Stream.concat(firstPageMessages.stream(), secondPageMessages.stream())));

    when(messagesManager.delete(eq(accountUuid), eq(device), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch queueEmptyLatch = new CountDownLatch(1);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class)))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(successResponse));

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
  void testProcessStoredMessagesMultiplePagesBackpressure() {
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    when(account.getNumber()).thenReturn("+18005551234");
    final UUID accountUuid = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    // Create two publishers, each with >2x WebSocketConnection.MESSAGE_SENDER_MAX_CONCURRENCY messages
    final TestPublisher<Envelope> firstPublisher = TestPublisher.createCold();
    final List<Envelope> firstPublisherMessages = IntStream.range(1,
            2 * WebSocketConnection.MESSAGE_SENDER_MAX_CONCURRENCY + 23)
        .mapToObj(i -> createMessage(UUID.randomUUID(), UUID.randomUUID(), i, "content " + i))
        .toList();

    final TestPublisher<Envelope> secondPublisher = TestPublisher.createCold();
    final List<Envelope> secondPublisherMessages = IntStream.range(firstPublisherMessages.size(),
            firstPublisherMessages.size() + 2 * WebSocketConnection.MESSAGE_SENDER_MAX_CONCURRENCY + 73)
        .mapToObj(i -> createMessage(UUID.randomUUID(), UUID.randomUUID(), i, "content " + i))
        .toList();

    final Flux<Envelope> allMessages = Flux.concat(firstPublisher, secondPublisher);
    when(messagesManager.getMessagesForDeviceReactive(accountUuid, device, false))
        .thenReturn(allMessages);

    when(messagesManager.delete(eq(accountUuid), eq(device), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch queueEmptyLatch = new CountDownLatch(1);

    final Queue<CompletableFuture<WebSocketResponseMessage>> pendingClientAcks = new LinkedList<>();

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class)))
        .thenAnswer(invocation -> {
          final CompletableFuture<WebSocketResponseMessage> pendingAck = new CompletableFuture<>();
          pendingClientAcks.add(pendingAck);
          return pendingAck;
        });

    when(client.sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty())))
        .thenAnswer(invocation -> {
          queueEmptyLatch.countDown();
          return CompletableFuture.completedFuture(successResponse);
        });

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      // start processing
      connection.processStoredMessages();

      firstPublisher.assertWasRequested();
      // emit all messages from the first publisher
      firstPublisher.emit(firstPublisherMessages.toArray(new Envelope[]{}));
      // nothing should be requested from the second publisher, because max concurrency is less than the number emitted,
      // and none have completed
      secondPublisher.assertWasNotRequested();
      // there should only be MESSAGE_SENDER_MAX_CONCURRENCY pending client acknowledgements
      assertEquals(WebSocketConnection.MESSAGE_SENDER_MAX_CONCURRENCY, pendingClientAcks.size());

      while (!pendingClientAcks.isEmpty()) {
        pendingClientAcks.poll().complete(successResponse);
      }

      secondPublisher.assertWasRequested();
      secondPublisher.emit(secondPublisherMessages.toArray(new Envelope[0]));

      while (!pendingClientAcks.isEmpty()) {
        pendingClientAcks.poll().complete(successResponse);
      }

      queueEmptyLatch.await();
    });

    verify(client, times(firstPublisherMessages.size() + secondPublisherMessages.size())).sendRequest(eq("PUT"),
        eq("/api/v1/message"), any(List.class), any(Optional.class));
    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  @Test
  void testProcessStoredMessagesContainsSenderUuid() {
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    when(account.getNumber()).thenReturn("+18005551234");
    final UUID accountUuid = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    final UUID senderUuid = UUID.randomUUID();
    final List<Envelope> messages = List.of(
        createMessage(senderUuid, UUID.randomUUID(), 1111L, "message the first"));

    when(messagesManager.getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false))
        .thenReturn(Flux.fromIterable(messages))
        .thenReturn(Flux.empty());

    when(messagesManager.delete(eq(accountUuid), eq(device), any(UUID.class), any()))
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
            if (!envelope.hasSourceServiceId() || envelope.getSourceServiceId().length() == 0) {
              return false;
            }
            return envelope.getSourceServiceId().equals(senderUuid.toString());
          } catch (InvalidProtocolBufferException e) {
            return false;
          }
        }));
    verify(client).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), any(List.class), eq(Optional.empty()));
  }

  private WebSocketConnection webSocketConnection(final WebSocketClient client) {
    return new WebSocketConnection(receiptSender, messagesManager, new MessageMetrics(),
        mock(PushNotificationManager.class), mock(PushNotificationScheduler.class), account, device, client,
        retrySchedulingExecutor, Schedulers.immediate(), clientReleaseManager,
        mock(MessageDeliveryLoopMonitor.class), mock(ExperimentEnrollmentManager.class));
  }

  @Test
  void testProcessStoredMessagesSingleEmptyCall() {
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(device), anyBoolean()))
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
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);
    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    final List<Envelope> firstPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 1111, "first"),
            createMessage(UUID.randomUUID(), UUID.randomUUID(), 2222, "second"));

    final List<Envelope> secondPageMessages =
        List.of(createMessage(UUID.randomUUID(), UUID.randomUUID(), 3333, "third"));

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(device), anyBoolean()))
        .thenReturn(Flux.fromIterable(firstPageMessages))
        .thenReturn(Flux.fromIterable(secondPageMessages))
        .thenReturn(Flux.empty());

    when(messagesManager.delete(eq(accountUuid), eq(device), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    final CountDownLatch queueEmptyLatch = new CountDownLatch(1);

    when(client.sendRequest(eq("PUT"), eq("/api/v1/message"), any(List.class), any(Optional.class)))
        .thenAnswer(invocation -> {
          connection.handleNewMessageAvailable();

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
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(device), anyBoolean()))
        .thenReturn(Flux.empty());

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    // This is a little hacky and non-obvious, but because we're always returning an empty list of messages, the call to
    // CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded future, and the
    // whenComplete method will get called immediately on THIS thread, so we don't need to synchronize or wait for
    // anything.
    connection.processStoredMessages();

    verify(messagesManager).getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false);

    connection.handleNewMessageAvailable();

    verify(messagesManager).getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, true);
  }

  @Test
  void testProcessDatabaseMessagesAfterPersist() {
    final WebSocketClient client = mock(WebSocketClient.class);
    final WebSocketConnection connection = webSocketConnection(client);

    final UUID accountUuid = UUID.randomUUID();

    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);
    when(client.isOpen()).thenReturn(true);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(device), anyBoolean()))
        .thenReturn(Flux.empty());

    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);

    // This is a little hacky and non-obvious, but because we're always returning an empty list of messages, the call to
    // CompletableFuture.allOf(...) in processStoredMessages will produce an instantly-succeeded future, and the
    // whenComplete method will get called immediately on THIS thread, so we don't need to synchronize or wait for
    // anything.
    connection.processStoredMessages();
    connection.handleMessagesPersisted();

    verify(messagesManager, times(2)).getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false);
  }

  @Test
  void testRetrieveMessageException() {
    UUID accountUuid = UUID.randomUUID();

    when(device.getId()).thenReturn((byte) 2);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    when(messagesManager.getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false))
        .thenReturn(Flux.error(new RedisException("OH NO")));

    when(retrySchedulingExecutor.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(
        (Answer<ScheduledFuture<?>>) invocation -> {
          invocation.getArgument(0, Runnable.class).run();
          return mock(ScheduledFuture.class);
        });

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);

    WebSocketConnection connection = webSocketConnection(client);
    connection.start();

    verify(retrySchedulingExecutor, times(WebSocketConnection.MAX_CONSECUTIVE_RETRIES)).schedule(any(Runnable.class),
        anyLong(), any());
    verify(client).close(eq(1011), anyString());
  }

  @Test
  void testRetrieveMessageExceptionClientDisconnected() {
    UUID accountUuid = UUID.randomUUID();

    when(device.getId()).thenReturn((byte) 2);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    when(messagesManager.getMessagesForDeviceReactive(account.getIdentifier(IdentityType.ACI), device, false))
        .thenReturn(Flux.error(new RedisException("OH NO")));

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(false);

    WebSocketConnection connection = webSocketConnection(client);
    connection.start();

    verify(retrySchedulingExecutor, never()).schedule(any(Runnable.class), anyLong(), any());
    verify(client, never()).close(anyInt(), anyString());
  }

  @Test
  void testReactivePublisherLimitRate() {
    final UUID accountUuid = UUID.randomUUID();

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

    final int totalMessages = 1000;

    final TestPublisher<Envelope> testPublisher = TestPublisher.createCold();
    final Flux<Envelope> flux = Flux.from(testPublisher);

    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(device), anyBoolean()))
        .thenReturn(flux);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);
    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);
    when(client.sendRequest(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));
    when(messagesManager.delete(any(), any(), any(), any())).thenReturn(
        CompletableFuture.completedFuture(Optional.empty()));

    WebSocketConnection connection = webSocketConnection(client);

    connection.start();

    StepVerifier.setDefaultTimeout(Duration.ofSeconds(5));

    StepVerifier.create(flux, 0)
        .expectSubscription()
        .thenRequest(totalMessages * 2)
        .then(() -> {
          for (long i = 0; i < totalMessages; i++) {
            testPublisher.next(createMessage(UUID.randomUUID(), accountUuid, 1111 * i + 1, "message " + i));
          }
          testPublisher.complete();
        })
        .expectNextCount(totalMessages)
        .expectComplete()
        .log()
        .verify();

    testPublisher.assertMaxRequested(WebSocketConnection.MESSAGE_PUBLISHER_LIMIT_RATE);
  }

  @Test
  void testReactivePublisherDisposedWhenConnectionStopped() {
    final UUID accountUuid = UUID.randomUUID();

    final byte deviceId = 2;
    when(device.getId()).thenReturn(deviceId);

    when(account.getNumber()).thenReturn("+14152222222");
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountUuid);

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
    when(messagesManager.getMessagesForDeviceReactive(eq(accountUuid), eq(device), anyBoolean()))
        .thenReturn(flux);

    final WebSocketClient client = mock(WebSocketClient.class);
    when(client.isOpen()).thenReturn(true);
    final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
    when(successResponse.getStatus()).thenReturn(200);
    when(client.sendRequest(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));
    when(messagesManager.delete(any(), any(), any(), any())).thenReturn(
        CompletableFuture.completedFuture(Optional.empty()));

    WebSocketConnection connection = webSocketConnection(client);

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
        .setClientTimestamp(timestamp)
        .setServerTimestamp(0)
        .setSourceServiceId(senderUuid.toString())
        .setSourceDevice(SOURCE_DEVICE_ID)
        .setDestinationServiceId(destinationUuid.toString())
        .setContent(ByteString.copyFrom(content.getBytes(StandardCharsets.UTF_8)))
        .build();
  }

}
