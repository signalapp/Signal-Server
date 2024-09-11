/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.ServiceId;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessagesConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

class MessagesCacheTest {

  private final Random random = new Random();
  private long serialTimestamp = 0;

  @Nested
  class WithRealCluster {

    @RegisterExtension
    static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

    private ExecutorService sharedExecutorService;
    private ScheduledExecutorService resubscribeRetryExecutorService;
    private Scheduler messageDeliveryScheduler;
    private MessagesCache messagesCache;

    private DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

    private static final UUID DESTINATION_UUID = UUID.randomUUID();

    private static final byte DESTINATION_DEVICE_ID = 7;

    @BeforeEach
    void setUp() throws Exception {

      REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
        connection.sync().flushall();
        connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz");
      });

      final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
      when(dynamicConfiguration.getMessagesConfiguration()).thenReturn(new DynamicMessagesConfiguration(true, true));
      dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
      when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

      sharedExecutorService = Executors.newSingleThreadExecutor();
      resubscribeRetryExecutorService = Executors.newSingleThreadScheduledExecutor();
      messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
      messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(), sharedExecutorService,
          messageDeliveryScheduler, sharedExecutorService, Clock.systemUTC(), dynamicConfigurationManager);

      messagesCache.start();
    }

    @AfterEach
    void tearDown() throws Exception {
      messagesCache.stop();

      sharedExecutorService.shutdown();
      sharedExecutorService.awaitTermination(1, TimeUnit.SECONDS);

      messageDeliveryScheduler.dispose();
      resubscribeRetryExecutorService.shutdown();
      resubscribeRetryExecutorService.awaitTermination(1, TimeUnit.SECONDS);
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

      final long firstId = messagesCache.insert(duplicateGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID,
          duplicateMessage);
      final long secondId = messagesCache.insert(duplicateGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID,
          duplicateMessage);

      assertEquals(firstId, secondId);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRemoveByUUID(final boolean sealedSender) throws Exception {
      final UUID messageGuid = UUID.randomUUID();

      assertEquals(Optional.empty(),
          messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageGuid).get(5, TimeUnit.SECONDS));

      final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

      messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);
      final Optional<RemovedMessage> maybeRemovedMessage = messagesCache.remove(DESTINATION_UUID,
          DESTINATION_DEVICE_ID, messageGuid).get(5, TimeUnit.SECONDS);

      assertEquals(Optional.of(RemovedMessage.fromEnvelope(message)), maybeRemovedMessage);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRemoveBatchByUUID(final boolean sealedSender) throws Exception {
      final int messageCount = 10;

      final List<MessageProtos.Envelope> messagesToRemove = new ArrayList<>(messageCount);
      final List<MessageProtos.Envelope> messagesToPreserve = new ArrayList<>(messageCount);

      for (int i = 0; i < 10; i++) {
        messagesToRemove.add(generateRandomMessage(UUID.randomUUID(), sealedSender));
        messagesToPreserve.add(generateRandomMessage(UUID.randomUUID(), sealedSender));
      }

      assertEquals(Collections.emptyList(), messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
          messagesToRemove.stream().map(message -> UUID.fromString(message.getServerGuid()))
              .collect(Collectors.toList())).get(5, TimeUnit.SECONDS));

      for (final MessageProtos.Envelope message : messagesToRemove) {
        messagesCache.insert(UUID.fromString(message.getServerGuid()), DESTINATION_UUID, DESTINATION_DEVICE_ID,
            message);
      }

      for (final MessageProtos.Envelope message : messagesToPreserve) {
        messagesCache.insert(UUID.fromString(message.getServerGuid()), DESTINATION_UUID, DESTINATION_DEVICE_ID,
            message);
      }

      final List<RemovedMessage> removedMessages = messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
          messagesToRemove.stream().map(message -> UUID.fromString(message.getServerGuid()))
              .collect(Collectors.toList())).get(5, TimeUnit.SECONDS);

      assertEquals(messagesToRemove.stream().map(RemovedMessage::fromEnvelope).toList(), removedMessages);
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

    @Test
    void testHasMessagesAsync() {
      assertFalse(messagesCache.hasMessagesAsync(DESTINATION_UUID, DESTINATION_DEVICE_ID).join());

      final UUID messageGuid = UUID.randomUUID();
      final MessageProtos.Envelope message = generateRandomMessage(messageGuid, true);
      messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

      assertTrue(messagesCache.hasMessagesAsync(DESTINATION_UUID, DESTINATION_DEVICE_ID).join());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testGetMessages(final boolean sealedSender) throws Exception {
      final int messageCount = 100;

      final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(messageCount);

      for (int i = 0; i < messageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);
        messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

        expectedMessages.add(message);
      }

      assertEquals(expectedMessages, get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));

      messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
          expectedMessages.stream()
              .map(MessageProtos.Envelope::getServerGuid)
              .map(UUID::fromString)
              .collect(Collectors.toList()));

      final UUID message1Guid = UUID.randomUUID();
      final MessageProtos.Envelope message1 = generateRandomMessage(message1Guid, sealedSender);
      messagesCache.insert(message1Guid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message1);
      final List<MessageProtos.Envelope> get1 = get(DESTINATION_UUID, DESTINATION_DEVICE_ID,
          1);
      assertEquals(List.of(message1), get1);

      messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID, message1Guid).get(5, TimeUnit.SECONDS);

      final UUID message2Guid = UUID.randomUUID();
      final MessageProtos.Envelope message2 = generateRandomMessage(message2Guid, sealedSender);

      messagesCache.insert(message2Guid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message2);

      assertEquals(List.of(message2), get(DESTINATION_UUID, DESTINATION_DEVICE_ID, 1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testGetMessagesPublisher(final boolean expectStale) throws Exception {
      final int messageCount = 214;

      final List<MessageProtos.Envelope> expectedMessages = new ArrayList<>(messageCount);

      for (int i = 0; i < messageCount; i++) {
        final UUID messageGuid = UUID.randomUUID();
        final MessageProtos.Envelope message = generateRandomMessage(messageGuid, true);
        messagesCache.insert(messageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, message);

        expectedMessages.add(message);
      }

      final UUID ephemeralMessageGuid = UUID.randomUUID();
      final MessageProtos.Envelope ephemeralMessage = generateRandomMessage(ephemeralMessageGuid, true)
          .toBuilder().setEphemeral(true).build();
      messagesCache.insert(ephemeralMessageGuid, DESTINATION_UUID, DESTINATION_DEVICE_ID, ephemeralMessage);

      final Clock cacheClock;
      if (expectStale) {
        cacheClock = Clock.fixed(Instant.ofEpochMilli(serialTimestamp + 1),
            ZoneId.of("Etc/UTC"));
      } else {
        cacheClock = Clock.fixed(
            Instant.ofEpochMilli(serialTimestamp + 1).plus(MessagesCache.MAX_EPHEMERAL_MESSAGE_DELAY),
            ZoneId.of("Etc/UTC"));
      }

      final MessagesCache messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
          sharedExecutorService, messageDeliveryScheduler, sharedExecutorService, cacheClock,
          dynamicConfigurationManager);

      final List<MessageProtos.Envelope> actualMessages = Flux.from(
              messagesCache.get(DESTINATION_UUID, DESTINATION_DEVICE_ID))
          .collectList()
          .block(Duration.ofSeconds(5));

      if (expectStale) {
        final List<MessageProtos.Envelope> expectedAllMessages = new ArrayList<>() {{
          addAll(expectedMessages);
          add(ephemeralMessage);
        }};

        assertEquals(expectedAllMessages, actualMessages);

      } else {
        assertEquals(expectedMessages, actualMessages);

        // delete all of these messages and call `getAll()`, to confirm that ephemeral messages have been discarded
        CompletableFuture.allOf(actualMessages.stream()
                .map(message -> messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
                    UUID.fromString(message.getServerGuid())))
                .toArray(CompletableFuture<?>[]::new))
            .get(5, TimeUnit.SECONDS);

        final List<MessageProtos.Envelope> messages = messagesCache.getAllMessages(DESTINATION_UUID,
                DESTINATION_DEVICE_ID)
            .collectList()
            .toFuture().get(5, TimeUnit.SECONDS);

        assertTrue(messages.isEmpty());
      }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClearQueueForDevice(final boolean sealedSender) {
      final int messageCount = 1000;

      for (final byte deviceId : new byte[]{DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1}) {
        for (int i = 0; i < messageCount; i++) {
          final UUID messageGuid = UUID.randomUUID();
          final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

          messagesCache.insert(messageGuid, DESTINATION_UUID, deviceId, message);
        }
      }

      messagesCache.clear(DESTINATION_UUID, DESTINATION_DEVICE_ID).join();

      assertEquals(Collections.emptyList(), get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
      assertEquals(messageCount, get(DESTINATION_UUID, (byte) (DESTINATION_DEVICE_ID + 1), messageCount).size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testClearQueueForAccount(final boolean sealedSender) {
      final int messageCount = 1000;

      for (final byte deviceId : new byte[]{DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1}) {
        for (int i = 0; i < messageCount; i++) {
          final UUID messageGuid = UUID.randomUUID();
          final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

          messagesCache.insert(messageGuid, DESTINATION_UUID, deviceId, message);
        }
      }

      messagesCache.clear(DESTINATION_UUID).join();

      assertEquals(Collections.emptyList(), get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
      assertEquals(Collections.emptyList(), get(DESTINATION_UUID, (byte) (DESTINATION_DEVICE_ID + 1), messageCount));
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
     * {@code false}. Its {@code counter} field tracks how many times {@code handleNewMessagesAvailable} has been
     * called.
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
        sharedExecutorService.submit(() -> listener1.firstMessageHandled.get()).get();

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMultiRecipientMessage(final boolean sharedMrmKeyPresent) throws Exception {
      final UUID destinationUuid = UUID.randomUUID();
      final byte deviceId = 1;

      final UUID mrmGuid = UUID.randomUUID();
      final SealedSenderMultiRecipientMessage mrm = generateRandomMrmMessage(
          new AciServiceIdentifier(destinationUuid), deviceId);

      final byte[] sharedMrmDataKey;
      if (sharedMrmKeyPresent) {
        sharedMrmDataKey = messagesCache.insertSharedMultiRecipientMessagePayload(mrmGuid, mrm);
      } else {
        sharedMrmDataKey = new byte[]{1};
      }

      final UUID guid = UUID.randomUUID();
      final MessageProtos.Envelope message = generateRandomMessage(guid, true)
          .toBuilder()
          // clear some things added by the helper
          .clearServerGuid()
          // mrm views phase 1: messages have content
          .setContent(
              ByteString.copyFrom(mrm.messageForRecipient(mrm.getRecipients().get(new ServiceId.Aci(destinationUuid)))))
          .setSharedMrmKey(ByteString.copyFrom(sharedMrmDataKey))
          .build();
      messagesCache.insert(guid, destinationUuid, deviceId, message);

      assertEquals(sharedMrmKeyPresent ? 1 : 0, (long) REDIS_CLUSTER_EXTENSION.getRedisCluster()
          .withBinaryCluster(conn -> conn.sync().exists(MessagesCache.getSharedMrmKey(mrmGuid))));

      final List<MessageProtos.Envelope> messages = get(destinationUuid, deviceId, 1);
      assertEquals(1, messages.size());
      assertEquals(guid, UUID.fromString(messages.getFirst().getServerGuid()));
      assertFalse(messages.getFirst().hasSharedMrmKey());

      final SealedSenderMultiRecipientMessage.Recipient recipient = mrm.getRecipients()
          .get(new ServiceId.Aci(destinationUuid));
      assertArrayEquals(mrm.messageForRecipient(recipient), messages.getFirst().getContent().toByteArray());

      final Optional<RemovedMessage> removedMessage = messagesCache.remove(destinationUuid, deviceId, guid)
          .join();

      assertTrue(removedMessage.isPresent());
      assertEquals(guid, UUID.fromString(removedMessage.get().serverGuid().toString()));
      assertTrue(get(destinationUuid, deviceId, 1).isEmpty());

      // updating the shared MRM data is purely async, so we just wait for it
      assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
        boolean exists;
        do {
          exists = 1 == REDIS_CLUSTER_EXTENSION.getRedisCluster()
              .withBinaryCluster(conn -> conn.sync().exists(MessagesCache.getSharedMrmKey(mrmGuid)));
        } while (exists);
      });
    }

    private List<MessageProtos.Envelope> get(final UUID destinationUuid, final byte destinationDeviceId,
        final int messageCount) {
      return Flux.from(messagesCache.get(destinationUuid, destinationDeviceId))
          .take(messageCount, true)
          .collectList()
          .block();
    }

  }

  @Nested
  class WithMockCluster {

    private MessagesCache messagesCache;
    private RedisAdvancedClusterReactiveCommands<byte[], byte[]> reactiveCommands;
    private RedisAdvancedClusterAsyncCommands<byte[], byte[]> asyncCommands;
    private Scheduler messageDeliveryScheduler;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() throws Exception {
      reactiveCommands = mock(RedisAdvancedClusterReactiveCommands.class);
      asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class);
      final FaultTolerantRedisCluster mockCluster = RedisClusterHelper.builder()
          .binaryReactiveCommands(reactiveCommands)
          .binaryAsyncCommands(asyncCommands)
          .build();

      messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");

      messagesCache = new MessagesCache(mockCluster, mock(ExecutorService.class), messageDeliveryScheduler,
          Executors.newSingleThreadExecutor(), Clock.systemUTC(), mock(DynamicConfigurationManager.class));
    }

    @AfterEach
    void teardown() {
      StepVerifier.resetDefaultTimeout();
      messageDeliveryScheduler.dispose();
    }

    @Test
    @Disabled("flaky test")
    void testGetAllMessagesLimitsAndBackpressure() {
      // this test makes sure that we don’t fetch and buffer all messages from the cache when the publisher
      // is subscribed. Rather, we should be fetching in pages to satisfy downstream requests, so that memory usage
      // is limited to few pages of messages

      // we use a combination of Flux.just() and TestPublishers to control when data is “fetched” and emitted from the
      // cache. The initial Flux.just()s are pages that are readily available, on demand. By design, there are more of
      // these pages than the initial prefetch. The publishers allow us to create extra demand but defer producing
      // values to satisfy the demand until later on.

      final TestPublisher<Object> page4Publisher = TestPublisher.create();
      final TestPublisher<Object> page56Publisher = TestPublisher.create();
      final TestPublisher<Object> emptyFinalPagePublisher = TestPublisher.create();

      final Deque<List<byte[]>> pages = new ArrayDeque<>();
      pages.add(generatePage());
      pages.add(generatePage());
      pages.add(generatePage());
      pages.add(generatePage());
      // make sure that stale ephemeral messages are also produced by calls to getAllMessages()
      pages.add(generateStaleEphemeralPage());
      pages.add(generatePage());

      when(reactiveCommands.evalsha(any(), any(), any(), any()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.from(page4Publisher))
          .thenReturn(Flux.from(page56Publisher))
          .thenReturn(Flux.from(emptyFinalPagePublisher))
          .thenReturn(Flux.empty());

      final Flux<?> allMessages = messagesCache.getAllMessages(UUID.randomUUID(), Device.PRIMARY_ID);

      // Why initialValue = 3?
      // 1. messagesCache.getAllMessages() above produces the first call
      // 2. when we subscribe, the prefetch of 1 results in `expand()`, which produces a second call
      // 3. there is an implicit “low tide mark” of 1, meaning there will be an extra call to replenish when there is
      //    1 value remaining
      final AtomicInteger expectedReactiveCommandInvocations = new AtomicInteger(3);

      StepVerifier.setDefaultTimeout(Duration.ofSeconds(5));

      final int page = 100;
      final int halfPage = page / 2;

      // in order to fully control demand and separate the prefetch mechanics, initially subscribe with a request of 0
      StepVerifier.create(allMessages, 0)
          .expectSubscription()
          .then(() -> verify(reactiveCommands, times(expectedReactiveCommandInvocations.get())).evalsha(any(), any(),
              any(), any()))
          .thenRequest(halfPage) // page 0.5 requested
          .expectNextCount(halfPage) // page 0.5 produced
          // page 0.5 produced, 1.5 remain, so no additional interactions with the cache cluster
          .then(() -> verify(reactiveCommands, atMost(expectedReactiveCommandInvocations.get())).evalsha(any(),
              any(), any(), any()))
          .then(page4Publisher::assertWasNotRequested)
          .thenRequest(page) // page 1.5 requested
          .expectNextCount(page) // page 1.5 produced

          // we now have produced 1.5 pages, have 0.5 buffered, and two more have been prefetched.
          // after producing more than a full page, we’ll need to replenish from the cache.
          // future requests will depend on sink emitters.
          // also NB: times() checks cumulative calls, hence addAndGet
          .then(() -> verify(reactiveCommands, times(expectedReactiveCommandInvocations.addAndGet(1))).evalsha(any(),
              any(), any(), any()))
          .then(page4Publisher::assertWasSubscribed)
          .thenRequest(page + halfPage) // page 3 requested
          .expectNextCount(page + halfPage) // page 1.5–3 produced

          .thenRequest(halfPage) // page 3.5 requested
          .then(page56Publisher::assertWasNotRequested)
          .then(() -> page4Publisher.emit(pages.pop()))
          .expectNextCount(halfPage) // page 3.5 produced
          .then(() -> verify(reactiveCommands, times(expectedReactiveCommandInvocations.addAndGet(1))).evalsha(any(),
              any(), any(), any()))
          .then(page56Publisher::assertWasSubscribed)

          .thenRequest(page) // page 4.5 requested
          .expectNextCount(halfPage) // page 4 produced

          .thenRequest(page * 4) // request more demand than we will ultimately satisfy

          .then(() -> page56Publisher.next(pages.pop()).next(pages.pop()).complete())
          .expectNextCount(page + page) // page 5 and 6 produced
          .then(emptyFinalPagePublisher::complete)
          // confirm that cache calls increased by 2: one for page 5-and-6 (we got a two-fer in next(pop()).next(pop()),
          //   and one for the final, empty page
          .then(() -> verify(reactiveCommands, times(expectedReactiveCommandInvocations.addAndGet(2))).evalsha(any(),
              any(), any(),
              any()))
          .expectComplete()
          .log()
          .verify();

      // make sure that we consumed all the pages, especially in case of refactoring
      assertTrue(pages.isEmpty());
    }

    @Test
    void testGetDiscardsEphemeralMessages() {
      final Deque<List<byte[]>> pages = new ArrayDeque<>();
      pages.add(generatePage());
      pages.add(generatePage());
      pages.add(generateStaleEphemeralPage());

      when(reactiveCommands.evalsha(any(), any(), any(byte[][].class), any(byte[][].class)))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.empty());

      final AsyncCommand<?, ?, ?> removeSuccess = new AsyncCommand<>(mock(RedisCommand.class));
      removeSuccess.complete();

      when(asyncCommands.evalsha(any(), any(), any(byte[][].class), any(byte[][].class)))
          .thenReturn((RedisFuture) removeSuccess);

      final Publisher<?> allMessages = messagesCache.get(UUID.randomUUID(), Device.PRIMARY_ID);

      StepVerifier.setDefaultTimeout(Duration.ofSeconds(5));

      // async commands are used for remove(), and nothing should happen until we are subscribed
      verify(asyncCommands, never()).evalsha(any(), any(), any(byte[][].class), any(byte[][].class));
      // the reactive commands will be called once, to prep the first page fetch (but no remote request would actually be sent)
      verify(reactiveCommands, times(1)).evalsha(any(), any(), any(byte[][].class), any(byte[][].class));

      StepVerifier.create(allMessages)
          .expectSubscription()
          .expectNextCount(200)
          .expectComplete()
          .log()
          .verify();

      assertTrue(pages.isEmpty());
      verify(asyncCommands, atLeast(1)).evalsha(any(), any(), any(byte[][].class), any(byte[][].class));
    }

    private List<byte[]> generatePage() {
      final List<byte[]> messagesAndIds = new ArrayList<>();

      for (int i = 0; i < 100; i++) {
        final MessageProtos.Envelope envelope = generateRandomMessage(UUID.randomUUID(), true);
        messagesAndIds.add(envelope.toByteArray());
        messagesAndIds.add(String.valueOf(serialTimestamp).getBytes());
      }

      return messagesAndIds;
    }

    private List<byte[]> generateStaleEphemeralPage() {
      final List<byte[]> messagesAndIds = new ArrayList<>();

      for (int i = 0; i < 100; i++) {
        final MessageProtos.Envelope envelope = generateRandomMessage(UUID.randomUUID(), true)
            .toBuilder().setEphemeral(true).build();
        messagesAndIds.add(envelope.toByteArray());
        messagesAndIds.add(String.valueOf(serialTimestamp).getBytes());
      }

      return messagesAndIds;
    }
  }

  private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final boolean sealedSender) {
    return generateRandomMessage(messageGuid, sealedSender, serialTimestamp++);
  }

  private MessageProtos.Envelope generateRandomMessage(final UUID messageGuid, final boolean sealedSender,
      final long timestamp) {
    final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(timestamp)
        .setServerTimestamp(timestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(messageGuid.toString())
        .setDestinationServiceId(UUID.randomUUID().toString());

    if (!sealedSender) {
      envelopeBuilder.setSourceDevice(random.nextInt(Device.MAXIMUM_DEVICE_ID) + 1)
          .setSourceServiceId(UUID.randomUUID().toString());
    }

    return envelopeBuilder.build();
  }

  static SealedSenderMultiRecipientMessage generateRandomMrmMessage(
      Map<AciServiceIdentifier, List<Byte>> destinations) {


    try {
      final ByteBuffer prefix = ByteBuffer.allocate(7);
      prefix.put((byte) 0x23); // version
      writeVarint(prefix, destinations.size()); // recipient count
      prefix.flip();

      List<ByteBuffer> recipients = new ArrayList<>(destinations.size());

      for (Map.Entry<AciServiceIdentifier, List<Byte>> aciServiceIdentifierAndDeviceIds : destinations.entrySet()) {

        final AciServiceIdentifier destination = aciServiceIdentifierAndDeviceIds.getKey();
        final List<Byte> deviceIds = aciServiceIdentifierAndDeviceIds.getValue();

        assert deviceIds.size() < 255;

        final ByteBuffer recipient = ByteBuffer.allocate(17 + 3 * deviceIds.size() + 48);

        recipient.put(destination.toFixedWidthByteArray());
        for (int i = 0; i < deviceIds.size(); i++) {
          final int hasMore = i == deviceIds.size() - 1 ? 0x0000 : 0x8000;
          recipient.put(new byte[]{deviceIds.get(i)}); // device ID
          recipient.putShort((short) ((100 + deviceIds.get(i)) | hasMore)); // registration ID
        }

        final byte[] keyMaterial = new byte[48];
        ThreadLocalRandom.current().nextBytes(keyMaterial);
        recipient.put(keyMaterial);

        recipients.add(recipient);
      }

      final byte[] commonPayload = new byte[64];
      ThreadLocalRandom.current().nextBytes(commonPayload);

      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(prefix.array(), 0, prefix.limit());
      for (ByteBuffer recipient : recipients) {
        baos.write(recipient.array());
      }
      baos.write(commonPayload);

      return SealedSenderMultiRecipientMessage.parse(baos.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static SealedSenderMultiRecipientMessage generateRandomMrmMessage(AciServiceIdentifier destination,
      byte... deviceIds) {

    final Map<AciServiceIdentifier, List<Byte>> destinations = new HashMap<>();
    destinations.put(destination, Arrays.asList(ArrayUtils.toObject(deviceIds)));
    return generateRandomMrmMessage(destinations);
  }

  private static void writeVarint(ByteBuffer bb, long n) {
    while (n >= 0x80) {
      bb.put((byte) (n & 0x7F | 0x80));
      n = n >> 7;
    }
    bb.put((byte) (n & 0x7F));
  }
}
