/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
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
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

class MessagesCacheTest {

  private final Random random = new Random();
  private long serialTimestamp = 0;

  @Nested
  class WithRealCluster {

    @RegisterExtension
    static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

    private ExecutorService sharedExecutorService;
    private MessagesCache messagesCache;

    private static final UUID DESTINATION_UUID = UUID.randomUUID();
    private static final int DESTINATION_DEVICE_ID = 7;

    @BeforeEach
    void setUp() throws Exception {

      REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection -> {
        connection.sync().flushall();
        connection.sync().upstream().commands().configSet("notify-keyspace-events", "K$glz");
      });

      sharedExecutorService = Executors.newSingleThreadExecutor();
      messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
          REDIS_CLUSTER_EXTENSION.getRedisCluster(), Clock.systemUTC(), sharedExecutorService,
          sharedExecutorService);

      messagesCache.start();
    }

    @AfterEach
    void tearDown() throws Exception {
      messagesCache.stop();

      sharedExecutorService.shutdown();
      sharedExecutorService.awaitTermination(1, TimeUnit.SECONDS);
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
      final Optional<MessageProtos.Envelope> maybeRemovedMessage = messagesCache.remove(DESTINATION_UUID,
          DESTINATION_DEVICE_ID, messageGuid).get(5, TimeUnit.SECONDS);

      assertEquals(Optional.of(message), maybeRemovedMessage);
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

      final List<MessageProtos.Envelope> removedMessages = messagesCache.remove(DESTINATION_UUID, DESTINATION_DEVICE_ID,
          messagesToRemove.stream().map(message -> UUID.fromString(message.getServerGuid()))
              .collect(Collectors.toList())).get(5, TimeUnit.SECONDS);

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
          REDIS_CLUSTER_EXTENSION.getRedisCluster(),
          cacheClock,
          sharedExecutorService,
          sharedExecutorService);

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
      final int messageCount = 100;

      for (final int deviceId : new int[]{DESTINATION_DEVICE_ID, DESTINATION_DEVICE_ID + 1}) {
        for (int i = 0; i < messageCount; i++) {
          final UUID messageGuid = UUID.randomUUID();
          final MessageProtos.Envelope message = generateRandomMessage(messageGuid, sealedSender);

          messagesCache.insert(messageGuid, DESTINATION_UUID, deviceId, message);
        }
      }

      messagesCache.clear(DESTINATION_UUID, DESTINATION_DEVICE_ID);

      assertEquals(Collections.emptyList(), get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
      assertEquals(messageCount, get(DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount).size());
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

      assertEquals(Collections.emptyList(), get(DESTINATION_UUID, DESTINATION_DEVICE_ID, messageCount));
      assertEquals(Collections.emptyList(), get(DESTINATION_UUID, DESTINATION_DEVICE_ID + 1, messageCount));
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

    private List<MessageProtos.Envelope> get(final UUID destinationUuid, final long destinationDeviceId,
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

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() throws Exception {
      reactiveCommands = mock(RedisAdvancedClusterReactiveCommands.class);
      asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class);

      final FaultTolerantRedisCluster mockCluster = RedisClusterHelper.builder()
          .binaryReactiveCommands(reactiveCommands)
          .binaryAsyncCommands(asyncCommands)
          .build();

      messagesCache = new MessagesCache(mockCluster, mockCluster, Clock.systemUTC(), mock(ExecutorService.class),
          Executors.newSingleThreadExecutor());
    }

    @AfterEach
    void teardown() {
      StepVerifier.resetDefaultTimeout();
    }

    @Test
    @Disabled("flaky test")
    void testGetAllMessagesLimitsAndBackpressure() {
      // this test makes sure that we don’t fetch and buffer all messages from the cache when the publisher
      // is subscribed. Rather, we should be fetching in pages to satisfy downstream requests, so that memory usage
      // is limited to few pages of messages

      // we use a combination of Flux.just() and Sinks to control when data is “fetched” from the cache. The initial
      // Flux.just()s are pages that are readily available, on demand. By design, there are more of these pages than
      // the initial prefetch. The sinks allow us to create extra demand but defer producing values to satisfy the demand
      // until later on.

      final AtomicReference<FluxSink<Object>> page4Sink = new AtomicReference<>();
      final AtomicReference<FluxSink<Object>> page56Sink = new AtomicReference<>();
      final AtomicReference<FluxSink<Object>> emptyFinalPageSink = new AtomicReference<>();

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
          .thenReturn(Flux.create(sink -> page4Sink.compareAndSet(null, sink)))
          .thenReturn(Flux.create(sink -> page56Sink.compareAndSet(null, sink)))
          .thenReturn(Flux.create(sink -> emptyFinalPageSink.compareAndSet(null, sink)))
          .thenReturn(Flux.empty());

      final Flux<?> allMessages = messagesCache.getAllMessages(UUID.randomUUID(), 1L);

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
          .then(() -> verify(reactiveCommands, times(expectedReactiveCommandInvocations.get())).evalsha(any(),
              any(), any(), any()))
          .then(() -> assertNull(page4Sink.get(), "page 4 should not have been fetched yet"))
          .thenRequest(page) // page 1.5 requested
          .expectNextCount(page) // page 1.5 produced

          // we now have produced 1.5 pages, have 0.5 buffered, and two more have been prefetched.
          // after producing more than a full page, we’ll need to replenish from the cache.
          // future requests will depend on sink emitters.
          // also NB: times() checks cumulative calls, hence addAndGet
          .then(() -> verify(reactiveCommands, times(expectedReactiveCommandInvocations.addAndGet(1))).evalsha(any(),
              any(), any(), any()))
          .then(() -> assertNotNull(page4Sink.get(), "page 4 should have been fetched"))
          .thenRequest(page + halfPage) // page 3 requested
          .expectNextCount(page + halfPage) // page 1.5–3 produced

          .thenRequest(halfPage) // page 3.5 requested
          .then(() -> assertNull(page56Sink.get(), "page 5 should not have been fetched yet"))
          .then(() -> page4Sink.get().next(pages.pop()).complete())
          .expectNextCount(halfPage) // page 3.5 produced
          .then(() -> verify(reactiveCommands, times(expectedReactiveCommandInvocations.addAndGet(1))).evalsha(any(),
              any(), any(), any()))
          .then(() -> assertNotNull(page56Sink.get(), "page 5 should have been fetched"))

          .thenRequest(page) // page 4.5 requested
          .expectNextCount(halfPage) // page 4 produced

          .thenRequest(page * 4) // request more demand than we will ultimately satisfy

          .then(() -> page56Sink.get().next(pages.pop()).next(pages.pop()).complete())
          .expectNextCount(page + page) // page 5 and 6 produced
          .then(() -> emptyFinalPageSink.get().complete())
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

      when(reactiveCommands.evalsha(any(), any(), any(), any()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.just(pages.pop()))
          .thenReturn(Flux.empty());

      final AsyncCommand<?, ?, ?> removeSuccess = new AsyncCommand<>(mock(RedisCommand.class));
      removeSuccess.complete();

      when(asyncCommands.evalsha(any(), any(), any(), any()))
          .thenReturn((RedisFuture) removeSuccess);

      final Publisher<?> allMessages = messagesCache.get(UUID.randomUUID(), 1L);

      StepVerifier.setDefaultTimeout(Duration.ofSeconds(5));

      // async commands are used for remove(), and nothing should happen until we are subscribed
      verify(asyncCommands, never()).evalsha(any(), any(), any(byte[][].class), any(byte[].class));
      // the reactive commands will be called once, to prep the first page fetch (but no remote request would actually be sent)
      verify(reactiveCommands, times(1)).evalsha(any(), any(), any(byte[][].class), any(byte[].class));

      StepVerifier.create(allMessages)
          .expectSubscription()
          .expectNextCount(200)
          .expectComplete()
          .log()
          .verify();

      assertTrue(pages.isEmpty());
      verify(asyncCommands, atLeast(1)).evalsha(any(), any(), any(), any());
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
}
