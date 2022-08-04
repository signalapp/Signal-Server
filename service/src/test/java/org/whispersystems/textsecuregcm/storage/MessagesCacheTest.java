/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.lettuce.core.cluster.SlotHash;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class MessagesCacheTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private ExecutorService notificationExecutorService;
  private MessagesCache messagesCache;

  private final Random random = new Random();
  private long serialTimestamp = 0;

  private static final UUID DESTINATION_UUID = UUID.randomUUID();
  private static final int DESTINATION_DEVICE_ID = 7;

  @BeforeEach
  void setUp() throws Exception {

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
      connection.sync().flushall();
      connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz");
    });

    notificationExecutorService = Executors.newSingleThreadExecutor();
    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(), REDIS_CLUSTER_EXTENSION.getRedisCluster(), notificationExecutorService);

    messagesCache.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    messagesCache.stop();

    notificationExecutorService.shutdown();
    notificationExecutorService.awaitTermination(1, TimeUnit.SECONDS);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testInsert(final boolean sealedSender) {
    final UUID messageGuid = UUID.randomUUID();
    assertTrue(messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID,
        generateRandomMessage(messageGuid, sealedSender)) > 0);
  }

  @Test
  void testDoubleInsertGuid() {
    final UUID duplicateGuid = UUID.randomUUID();
    final MessageProtos.Envelope duplicateMessage = generateRandomMessage(duplicateGuid, false);

    final long firstId = messagesCache.insert(duplicateGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, duplicateMessage);
    final long secondId = messagesCache.insert(duplicateGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID,
        duplicateMessage);

    assertEquals(firstId, secondId);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRemoveByUUID(final boolean sealedSender) {
    final UUID messageGuid = UUID.randomUUID();

    assertEquals(Optional.empty(), messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageGuid));

    final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

    messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
    final Optional<MessageProtos.Envelope> maybeRemovedMessage = messagesCache.remove(DESTINATION_UUID,
        DESTINATION_DEVICE_ID, messageGuid);

    assertEquals(Optional.of(message), maybeRemovedMessage);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRemoveBatchByUUID(final boolean sealedSender) {
    final int messageCount = 10;

    final List<MessageProtos.Envelope> messagesToRemove = new ArrayList<>(messageCount);
    final List<MessageProtos.Envelope> messagesToPreserve = new ArrayList<>(messageCount);

    for (int i = 0; i < 10; i++) {
      messagesToRemove.add(generateRandomMessage(UUID.randomUUID(), sealedSender));
      messagesToPreserve.add(generateRandomMessage(UUID.randomUUID(), sealedSender));
    }

    assertEquals(Collections.emptyList(), messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
        messagesToRemove.stream().map(message -> UUID.fromString(message.getServerGuid()))
            .collect(Collectors.toList())));

    for (final MessageProtos.Envelope message : messagesToRemove) {
      messagesCache.insert(UUID.fromString(message.getServerGuid()), DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
    }

    for (final MessageProtos.Envelope message : messagesToPreserve) {
      messagesCache.insert(UUID.fromString(message.getServerGuid()), DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
    }

    final List<MessageProtos.Envelope> removedMessages = messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
        messagesToRemove.stream().map(message -> UUID.fromString(message.getServerGuid()))
            .collect(Collectors.toList()));

    assertEquals(messagesToRemove, removedMessages);
    assertEquals(messagesToPreserve,
        messagesCache.getMessagesToPersist(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
  }

  @Test
  void testHasMessages() {
    assertFalse(messagesCache.hasMessages(DESTINATION_UUID, DESTINATION_DEVICE_ID));

    final UUID messageGuid = UUID.randomUUID();
    final MessageProtos.Envelope message = generateRandomMessage(messageGuid, true);
    messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

    assertTrue(messagesCache.hasMessages(DESTINATION_UUID, DESTINATION_DEVICE_ID));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetMessages(final boolean sealedSender) {
    final int messageCount = 100;

    final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(messageCount);

    for (int i = 0; i < messageCount; i++) {
      final UUID messageGuid = UUID.randomUUID();
      final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);
      messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

      expectedMessages.add(message);
    }

    assertEquals(expectedMessages, messagesCache.get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testClearQueueForDevice(final boolean sealedSender) {
    final int messageCount = 100;

    for (final int deviceId : new int[]{DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1}) {
      for (int i = 0; i < messageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

        messagesCache.insert(messageGuid, DESTINATION_UUID, deviceId, message);
      }
    }

    messagesCache.clear(DESTINATION_UUID, DESTINATION_DEVICE_ID);

    assertEquals(Collections.emptyList(), messagesCache.get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
    assertEquals(messageCount, messagesCache.get(DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount).size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testClearQueueForAccount(final boolean sealedSender) {
    final int messageCount = 100;

    for (final int deviceId : new int[]{DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1}) {
      for (int i = 0; i < messageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

        messagesCache.insert(messageGuid, DESTINATION_UUID, deviceId, message);
      }
    }

    messagesCache.clear(DESTINATION_UUID);

    assertEquals(Collections.emptyList(), messagesCache.get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
    assertEquals(Collections.emptyList(), messagesCache.get(DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount));
  }

  private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final boolean sealedSender) {
    return generateRandomMessage(messageGuid, sealedSender, serialTimestamp++);
  }

  private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final boolean sealedSender,
      final long timestamp) {
    final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
        .setTimestamp(timestamp)
        .setServerTimestamp(timestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(messageGuid.toString())
        .setDestinationUuid(UUID.randomUUID().toString());

    if (!sealedSender) {
      envelopeBuilder.setSourceDevice(random.nextInt(256))
          .setSourceUuid(UUID.randomUUID().toString());
    }

    return envelopeBuilder.build();
  }

  @Test
  void testClearNullUuid() {
    // We're happy as long as this doesn't throw an exception
    messagesCache.clear(null);
  }

  @Test
  void testGetAccountFromQueueName() {
    assertEquals(DESTINATION_UUID,
        MessagesCache.getAccountUuidFromQueueName(
            new String(MessagesCache.getMessageQueueKey(DESTINATION_UUID, DESTINATION_DEVICE_ID),
                StandardCharsets.UTF_8)));
  }

  @Test
  void testGetDeviceIdFromQueueName() {
    assertEquals(DESTINATION_DEVICE_ID,
        MessagesCache.getDeviceIdFromQueueName(
            new String(MessagesCache.getMessageQueueKey(DESTINATION_UUID, DESTINATION_DEVICE_ID),
                StandardCharsets.UTF_8)));
  }

  @Test
  void testGetQueueNameFromKeyspaceChannel() {
    assertEquals("1b363a31-a429-4fb6-8959-984a025e72ff::7",
        MessagesCache.getQueueNameFromKeyspaceChannel(
            "__keyspace@0__:user_queue::{1b363a31-a429-4fb6-8959-984a025e72ff::7}"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetQueuesToPersist(final boolean sealedSender) {
    final UUID messageGuid = UUID.randomUUID();

    messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID,
        generateRandomMessage(messageGuid, sealedSender));
    final int slot = SlotHash.getSlot(DESTINATION_UUID + "::" + DESTINATION_DEVICE_ID);

    assertTrue(messagesCache.getQueuesToPersist(slot + 1, Instant.now().plusSeconds(60), 100).isEmpty());

    final List<String> queues = messagesCache.getQueuesToPersist(slot, Instant.now().plusSeconds(60), 100);

    assertEquals(1, queues.size());
    assertEquals(DESTINATION_UUID, MessagesCache.getAccountUuidFromQueueName(queues.get(0)));
    assertEquals(DESTINATION_DEVICE_ID, MessagesCache.getDeviceIdFromQueueName(queues.get(0)));
  }

  @Test
  void testNotifyListenerNewMessage() {
    final AtomicBoolean notified = new AtomicBoolean(false);
    final UUID messageGuid = UUID.randomUUID();

    final MessageAvailabilityListener listener = new MessageAvailabilityListener() {
      @Override
      public boolean handleNewMessagesAvailable() {
        synchronized (notified) {
          notified.set(true);
          notified.notifyAll();

          return true;
        }
      }

      @Override
      public boolean handleMessagesPersisted() {
        return true;
      }
    };

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      messagesCache.addMessageAvailabilityListener(DESTINATION_UUID, DESTINATION_DEVICE_ID, listener);
      messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID,
          generateRandomMessage(messageGuid, true));

      synchronized (notified) {
        while (!notified.get()) {
          notified.wait();
        }
      }

      assertTrue(notified.get());
    });
  }

  @Test
  void testNotifyListenerPersisted() {
    final AtomicBoolean notified = new AtomicBoolean(false);

    final MessageAvailabilityListener listener = new MessageAvailabilityListener() {
      @Override
      public boolean handleNewMessagesAvailable() {
        return true;
      }

      @Override
      public boolean handleMessagesPersisted() {
        synchronized (notified) {
          notified.set(true);
          notified.notifyAll();

          return true;
        }
      }
    };

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      messagesCache.addMessageAvailabilityListener(DESTINATION_UUID, DESTINATION_DEVICE_ID, listener);

      messagesCache.lockQueueForPersistence(DESTINATION_UUID, DESTINATION_DEVICE_ID);
      messagesCache.unlockQueueForPersistence(DESTINATION_UUID, DESTINATION_DEVICE_ID);

      synchronized (notified) {
        while (!notified.get()) {
          notified.wait();
        }
      }

      assertTrue(notified.get());
    });
  }


  /**
   * Helper class that implements {@link MessageAvailabilityListener#handleNewMessagesAvailable()} by always returning
   * {@code false}. Its {@code counter} field tracks how many times {@code handleNewMessagesAvailable} has been called.
   * <p>
   * It uses a {@link CompletableFuture} to signal that it has received a “messages available” callback for the first
   * time.
   */
  private static class NewMessagesAvailabilityClosedListener implements MessageAvailabilityListener {

    private int counter;

    private final Consumer<Integer> messageHandledCallback;
    private final CompletableFuture<Void> firstMessageHandled = new CompletableFuture<>();

    private NewMessagesAvailabilityClosedListener(final Consumer<Integer> messageHandledCallback) {
      this.messageHandledCallback = messageHandledCallback;
    }

    @Override
    public boolean handleNewMessagesAvailable() {
      counter++;
      messageHandledCallback.accept(counter);
      firstMessageHandled.complete(null);

      return false;

    }

    @Override
    public boolean handleMessagesPersisted() {
      return true;
    }
  }

  @Test
  void testAvailabilityListenerResponses() {
    final NewMessagesAvailabilityClosedListener listener1 = new NewMessagesAvailabilityClosedListener(
        count -> assertEquals(1, count));
    final NewMessagesAvailabilityClosedListener listener2 = new NewMessagesAvailabilityClosedListener(
        count -> assertEquals(1, count));

    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      messagesCache.addMessageAvailabilityListener(DESTINATION_UUID, DESTINATION_DEVICE_ID, listener1);
      final UUID messageGuid1 = UUID.randomUUID();
      messagesCache.insert(messageGuid1, DESTINATION_UUID, DESTINATION_DEVICE_ID,
          generateRandomMessage(messageGuid1, true));

      listener1.firstMessageHandled.get();

      // Avoid a race condition by blocking on the message handled future *and* the current notification executor task—
      // the notification executor task includes unsubscribing `listener1`, and, if we don’t wait, sometimes
      // `listener2` will get subscribed before `listener1` is cleaned up
      notificationExecutorService.submit(() -> listener1.firstMessageHandled.get()).get();

      final UUID messageGuid2 = UUID.randomUUID();
      messagesCache.insert(messageGuid2, DESTINATION_UUID, DESTINATION_DEVICE_ID,
          generateRandomMessage(messageGuid2, true));

      messagesCache.addMessageAvailabilityListener(DESTINATION_UUID, DESTINATION_DEVICE_ID, listener2);

      final UUID messageGuid3 = UUID.randomUUID();
      messagesCache.insert(messageGuid3, DESTINATION_UUID, DESTINATION_DEVICE_ID,
          generateRandomMessage(messageGuid3, true));

      listener2.firstMessageHandled.get();
    });
  }
}
