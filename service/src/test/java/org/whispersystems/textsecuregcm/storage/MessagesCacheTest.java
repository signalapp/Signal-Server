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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
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
    final Optional<OutgoingMessageEntity> maybeRemovedMessage = messagesCache.remove(DESTINATION_UUID,
        DESTINATION_DEVICE_ID, messageGuid);

    assertTrue(maybeRemovedMessage.isPresent());
    assertEquals(MessagesCache.constructEntityFromEnvelope(message), maybeRemovedMessage.get());
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

    final List<OutgoingMessageEntity> removedMessages = messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
        messagesToRemove.stream().map(message -> UUID.fromString(message.getServerGuid()))
            .collect(Collectors.toList()));

    assertEquals(messagesToRemove.stream().map(MessagesCache::constructEntityFromEnvelope)
            .collect(Collectors.toList()),
        removedMessages);

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

    final List<OutgoingMessageEntity> expectedMessages = new ArrayList<>(messageCount);

    for (int i = 0; i < messageCount; i++) {
      final UUID messageGuid = UUID.randomUUID();
      final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);
      final long messageId = messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

      expectedMessages.add(MessagesCache.constructEntityFromEnvelope(message));
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
          .setSource("+1" + RandomStringUtils.randomNumeric(10));
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
      public void handleNewMessagesAvailable() {
        synchronized (notified) {
          notified.set(true);
          notified.notifyAll();
        }
      }

      @Override
      public void handleMessagesPersisted() {
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
      public void handleNewMessagesAvailable() {
      }

      @Override
      public void handleMessagesPersisted() {
        synchronized (notified) {
          notified.set(true);
          notified.notifyAll();
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

}
