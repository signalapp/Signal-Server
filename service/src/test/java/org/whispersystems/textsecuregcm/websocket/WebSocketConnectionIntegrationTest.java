/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesCache;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.tests.util.MessagesDynamoDbRule;
import org.whispersystems.websocket.WebSocketClient;
import org.whispersystems.websocket.messages.WebSocketResponseMessage;

public class WebSocketConnectionIntegrationTest extends AbstractRedisClusterTest {

    @Rule
    public MessagesDynamoDbRule messagesDynamoDbRule = new MessagesDynamoDbRule();

    private ExecutorService executorService;
    private MessagesDynamoDb messagesDynamoDb;
    private MessagesCache messagesCache;
    private ReportMessageManager reportMessageManager;
    private Account account;
    private Device device;
    private WebSocketClient webSocketClient;
    private WebSocketConnection webSocketConnection;
    private ScheduledExecutorService retrySchedulingExecutor;

    private long serialTimestamp = System.currentTimeMillis();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        executorService = Executors.newSingleThreadExecutor();
        messagesCache = new MessagesCache(getRedisCluster(), getRedisCluster(), executorService);
        messagesDynamoDb = new MessagesDynamoDb(messagesDynamoDbRule.getDynamoDB(), MessagesDynamoDbRule.TABLE_NAME, Duration.ofDays(7));
        reportMessageManager = mock(ReportMessageManager.class);
        account = mock(Account.class);
        device = mock(Device.class);
        webSocketClient = mock(WebSocketClient.class);
        retrySchedulingExecutor = Executors.newSingleThreadScheduledExecutor();

        when(account.getNumber()).thenReturn("+18005551234");
        when(account.getUuid()).thenReturn(UUID.randomUUID());
        when(device.getId()).thenReturn(1L);

        webSocketConnection = new WebSocketConnection(
                mock(ReceiptSender.class),
                new MessagesManager(messagesDynamoDb, messagesCache, mock(PushLatencyManager.class), reportMessageManager),
                account,
                device,
                webSocketClient,
                retrySchedulingExecutor);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);

        retrySchedulingExecutor.shutdown();
        retrySchedulingExecutor.awaitTermination(2, TimeUnit.SECONDS);

        super.tearDown();
    }

    @Test(timeout = 15_000)
    public void testProcessStoredMessages() throws InterruptedException {
        final int persistedMessageCount = 207;
        final int cachedMessageCount    = 173;

        final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(persistedMessageCount + cachedMessageCount);

        {
            final List<MessageProtos.Envelope> persistedMessages = new ArrayList<>(persistedMessageCount);

            for (int i = 0; i < persistedMessageCount; i++) {
                final MessageProtos.Envelope envelope = generateRandomMessage(UUID.randomUUID());

                persistedMessages.add(envelope);
                expectedMessages.add(envelope);
            }

            messagesDynamoDb.store(persistedMessages, account.getUuid(), device.getId());
        }

        for (int i = 0; i < cachedMessageCount; i++) {
            final UUID messageGuid = UUID.randomUUID();
            final MessageProtos.Envelope envelope = generateRandomMessage(messageGuid);

            messagesCache.insert(messageGuid, account.getUuid(), device.getId(), envelope);
            expectedMessages.add(envelope);
        }

        final WebSocketResponseMessage successResponse = mock(WebSocketResponseMessage.class);
        final AtomicBoolean queueCleared = new AtomicBoolean(false);

        when(successResponse.getStatus()).thenReturn(200);
        when(webSocketClient.sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        when(webSocketClient.sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), any())).thenAnswer((Answer<CompletableFuture<WebSocketResponseMessage>>)invocation -> {
            synchronized (queueCleared) {
                queueCleared.set(true);
                queueCleared.notifyAll();
            }

            return CompletableFuture.completedFuture(successResponse);
        });

        webSocketConnection.processStoredMessages();

        synchronized (queueCleared) {
            while (!queueCleared.get()) {
                queueCleared.wait();
            }
        }

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Optional<byte[]>> messageBodyCaptor = ArgumentCaptor.forClass(Optional.class);

        verify(webSocketClient, times(persistedMessageCount + cachedMessageCount)).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), messageBodyCaptor.capture());
        verify(webSocketClient).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));

        final List<MessageProtos.Envelope> sentMessages = new ArrayList<>();

        for (final Optional<byte[]> maybeMessageBody : messageBodyCaptor.getAllValues()) {
            maybeMessageBody.ifPresent(messageBytes -> {
                try {
                    sentMessages.add(MessageProtos.Envelope.parseFrom(messageBytes));
                } catch (final InvalidProtocolBufferException e) {
                    fail("Could not parse sent message");
                }
            });
        }

        assertEquals(expectedMessages, sentMessages);
    }

    @Test(timeout = 15_000)
    public void testProcessStoredMessagesClientClosed() {
        final int persistedMessageCount = 207;
        final int cachedMessageCount = 173;

        final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(persistedMessageCount + cachedMessageCount);

        {
            final List<MessageProtos.Envelope> persistedMessages = new ArrayList<>(persistedMessageCount);

            for (int i = 0; i < persistedMessageCount; i++) {
                final MessageProtos.Envelope envelope = generateRandomMessage(UUID.randomUUID());
                persistedMessages.add(envelope);
                expectedMessages.add(envelope);
            }

            messagesDynamoDb.store(persistedMessages, account.getUuid(), device.getId());
        }

        for (int i = 0; i < cachedMessageCount; i++) {
            final UUID messageGuid = UUID.randomUUID();
            final MessageProtos.Envelope envelope = generateRandomMessage(messageGuid);
            messagesCache.insert(messageGuid, account.getUuid(), device.getId(), envelope);

            expectedMessages.add(envelope);
        }

        when(webSocketClient.sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), any())).thenReturn(CompletableFuture.failedFuture(new IOException("Connection closed")));

        webSocketConnection.processStoredMessages();

        //noinspection unchecked
        ArgumentCaptor<Optional<byte[]>> messageBodyCaptor = ArgumentCaptor.forClass(Optional.class);

        verify(webSocketClient, atMost(persistedMessageCount + cachedMessageCount)).sendRequest(eq("PUT"), eq("/api/v1/message"), anyList(), messageBodyCaptor.capture());
        verify(webSocketClient, never()).sendRequest(eq("PUT"), eq("/api/v1/queue/empty"), anyList(), eq(Optional.empty()));

      final List<MessageProtos.Envelope> sentMessages = messageBodyCaptor.getAllValues().stream()
          .map(Optional::get)
          .map(messageBytes -> {
              try {
                  return Envelope.parseFrom(messageBytes);
              } catch (InvalidProtocolBufferException e) {
                  throw new RuntimeException(e);
              }
          })
          .collect(Collectors.toList());

      assertTrue(expectedMessages.containsAll(sentMessages));
    }

    private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid) {
        final long timestamp = serialTimestamp++;

        return MessageProtos.Envelope.newBuilder()
                .setTimestamp(timestamp)
                .setServerTimestamp(timestamp)
                .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                .setServerGuid(messageGuid.toString())
                .build();
    }
}
