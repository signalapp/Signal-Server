/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.MockUtils.exactly;

import com.google.protobuf.ByteString;
import io.lettuce.core.cluster.SlotHash;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessagePersisterConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.model.ItemCollectionSizeLimitExceededException;

@Timeout(value = 15, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MessagePersisterTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private ExecutorService sharedExecutorService;
  private ScheduledExecutorService resubscribeRetryExecutorService;
  private Scheduler messageDeliveryScheduler;
  private MessagesCache messagesCache;
  private MessagesDynamoDb messagesDynamoDb;
  private MessagePersister messagePersister;
  private AccountsManager accountsManager;
  private MessagesManager messagesManager;
  private Account destinationAccount;

  private static final UUID DESTINATION_ACCOUNT_UUID = UUID.randomUUID();
  private static final String DESTINATION_ACCOUNT_NUMBER = "+18005551234";
  private static final byte DESTINATION_DEVICE_ID = 7;
  private static final Device DESTINATION_DEVICE = DevicesHelper.createDevice(DESTINATION_DEVICE_ID);

  private static final Duration PERSIST_DELAY = Duration.ofMinutes(5);

  private static final double EXTRA_ROOM_RATIO = 2.0;

  @BeforeEach
  void setUp() throws Exception {

    messagesManager = mock(MessagesManager.class);
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    messagesDynamoDb = mock(MessagesDynamoDb.class);
    accountsManager = mock(AccountsManager.class);
    destinationAccount = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(DESTINATION_ACCOUNT_UUID)).thenReturn(Optional.of(destinationAccount));
    when(accountsManager.removeDevice(any(), anyByte()))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(invocation.getArgument(0)));

    when(destinationAccount.getUuid()).thenReturn(DESTINATION_ACCOUNT_UUID);
    when(destinationAccount.getIdentifier(IdentityType.ACI)).thenReturn(DESTINATION_ACCOUNT_UUID);
    when(destinationAccount.getNumber()).thenReturn(DESTINATION_ACCOUNT_NUMBER);
    when(destinationAccount.getDevice(DESTINATION_DEVICE_ID)).thenReturn(Optional.of(DESTINATION_DEVICE));

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfiguration.getMessagePersisterConfiguration())
        .thenReturn(new DynamicMessagePersisterConfiguration(true, EXTRA_ROOM_RATIO));
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    sharedExecutorService = Executors.newSingleThreadExecutor();
    resubscribeRetryExecutorService = Executors.newSingleThreadScheduledExecutor();
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        messageDeliveryScheduler, sharedExecutorService, mock(ScheduledExecutorService.class), Clock.systemUTC(), mock(ExperimentEnrollmentManager.class));
    messagePersister = new MessagePersister(messagesCache, messagesManager, accountsManager,
        dynamicConfigurationManager, PERSIST_DELAY, 1);

    when(messagesManager.clear(any(UUID.class), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    when(messagesManager.persistMessages(any(UUID.class), any(), any())).thenAnswer(invocation -> {
      final UUID destinationUuid = invocation.getArgument(0);
      final Device destinationDevice = invocation.getArgument(1);
      final List<MessageProtos.Envelope> messages = invocation.getArgument(2);

      messagesDynamoDb.store(messages, destinationUuid, destinationDevice);

      for (final MessageProtos.Envelope message : messages) {
        messagesCache.remove(destinationUuid, destinationDevice.getId(), UUID.fromString(message.getServerGuid())).get();
      }

      return messages.size();
    });
  }

  @AfterEach
  void tearDown() throws Exception {
    sharedExecutorService.shutdown();
    sharedExecutorService.awaitTermination(1, TimeUnit.SECONDS);

    messageDeliveryScheduler.dispose();
    resubscribeRetryExecutorService.shutdown();
    resubscribeRetryExecutorService.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void testPersistNextQueuesNoQueues() {
    messagePersister.persistNextQueues(Instant.now());

    verify(accountsManager, never()).getByAccountIdentifier(any(UUID.class));
  }

  @Test
  void testPersistNextQueuesSingleQueue() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = (MessagePersister.MESSAGE_BATCH_LIMIT * 3) + 7;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    messagePersister.persistNextQueues(now.plus(messagePersister.getPersistDelay()));

    final ArgumentCaptor<List<MessageProtos.Envelope>> messagesCaptor = ArgumentCaptor.forClass(List.class);

    verify(messagesDynamoDb, atLeastOnce()).store(messagesCaptor.capture(), eq(DESTINATION_ACCOUNT_UUID),
        eq(DESTINATION_DEVICE));
    assertEquals(messageCount, messagesCaptor.getAllValues().stream().mapToInt(List::size).sum());
  }

  @Test
  void testPersistNextQueuesSingleQueueTooSoon() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = (MessagePersister.MESSAGE_BATCH_LIMIT * 3) + 7;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    messagePersister.persistNextQueues(now);

    verify(messagesDynamoDb, never()).store(any(), any(), any());
  }

  @Test
  void testPersistNextQueuesMultiplePages() {
    final int slot = 7;
    final int queueCount = (MessagePersister.QUEUE_BATCH_LIMIT * 3) + 7;
    final int messagesPerQueue = 10;
    final Instant now = Instant.now();

    for (int i = 0; i < queueCount; i++) {
      final String queueName = generateRandomQueueNameForSlot(slot);
      final UUID accountUuid = MessagesCache.getAccountUuidFromQueueName(queueName);
      final byte deviceId = MessagesCache.getDeviceIdFromQueueName(queueName);
      final String accountNumber = "+1" + RandomStringUtils.randomNumeric(10);

      final Account account = mock(Account.class);

      when(accountsManager.getByAccountIdentifier(accountUuid)).thenReturn(Optional.of(account));
      when(account.getUuid()).thenReturn(accountUuid);
      when(account.getNumber()).thenReturn(accountNumber);
      when(account.getDevice(anyByte())).thenAnswer(invocation -> Optional.of(DevicesHelper.createDevice(invocation.getArgument(0))));

      insertMessages(accountUuid, deviceId, messagesPerQueue, now);
    }

    setNextSlotToPersist(slot);

    messagePersister.persistNextQueues(now.plus(messagePersister.getPersistDelay()));

    final ArgumentCaptor<List<MessageProtos.Envelope>> messagesCaptor = ArgumentCaptor.forClass(List.class);

    verify(messagesDynamoDb, atLeastOnce()).store(messagesCaptor.capture(), any(UUID.class), any());
    assertEquals(queueCount * messagesPerQueue, messagesCaptor.getAllValues().stream().mapToInt(List::size).sum());
  }

  @Test
  void testPersistQueueRetry() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = (MessagePersister.MESSAGE_BATCH_LIMIT * 3) + 7;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    doAnswer((Answer<Void>) invocation -> {
      throw new RuntimeException("OH NO.");
        }).when(messagesDynamoDb).store(any(), eq(DESTINATION_ACCOUNT_UUID), eq(DESTINATION_DEVICE));

    messagePersister.persistNextQueues(now.plus(messagePersister.getPersistDelay()));

    assertEquals(List.of(queueName),
        messagesCache.getQueuesToPersist(SlotHash.getSlot(queueName),
            Instant.now().plus(messagePersister.getPersistDelay()), 1));
  }

  @Test
  void testPersistQueueRetryLoop() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = (MessagePersister.MESSAGE_BATCH_LIMIT * 3) + 7;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    // returning `0` indicates something not working correctly
    when(messagesManager.persistMessages(any(UUID.class), any(), anyList())).thenReturn(0);

    assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
        assertThrows(MessagePersistenceException.class,
            () -> messagePersister.persistQueue(destinationAccount, DESTINATION_DEVICE, "test")));
  }

  @Test
  void testUnlinkOnFullQueue() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = 1;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    final Device primary = mock(Device.class);
    when(primary.getId()).thenReturn((byte) 1);
    when(primary.isPrimary()).thenReturn(true);
    when(primary.getFetchesMessages()).thenReturn(true);

    final Device activeA = mock(Device.class);
    when(activeA.getId()).thenReturn((byte) 2);
    when(activeA.getFetchesMessages()).thenReturn(true);

    final Device inactiveB = mock(Device.class);
    final byte inactiveId = 3;
    when(inactiveB.getId()).thenReturn(inactiveId);

    final Device inactiveC = mock(Device.class);
    when(inactiveC.getId()).thenReturn((byte) 4);

    final Device activeD = mock(Device.class);
    when(activeD.getId()).thenReturn((byte) 5);
    when(activeD.getFetchesMessages()).thenReturn(true);

    final Device destination = mock(Device.class);
    when(destination.getId()).thenReturn(DESTINATION_DEVICE_ID);

    when(destinationAccount.getDevices()).thenReturn(List.of(primary, activeA, inactiveB, inactiveC, activeD, destination));

    when(messagesManager.persistMessages(any(UUID.class), any(), anyList())).thenThrow(ItemCollectionSizeLimitExceededException.builder().build());

    assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
        messagePersister.persistQueue(destinationAccount, DESTINATION_DEVICE, "test"));
    verify(accountsManager, exactly()).removeDevice(destinationAccount, DESTINATION_DEVICE_ID);
  }

  @Test
  void testTrimOnFullPrimaryQueue() {
    final byte[] queueName = MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, Device.PRIMARY_ID);
    final Instant now = Instant.now();

    final List<MessageProtos.Envelope> cachedMessages = Stream.generate(() -> generateMessage(
            DESTINATION_ACCOUNT_UUID, UUID.randomUUID(), now.getEpochSecond(), ThreadLocalRandom.current().nextInt(100)))
        .limit(10)
        .toList();
    final long cacheSize = cachedMessages.stream().mapToLong(MessageProtos.Envelope::getSerializedSize).sum();
    for (MessageProtos.Envelope envelope : cachedMessages) {
      messagesCache.insert(UUID.fromString(envelope.getServerGuid()), DESTINATION_ACCOUNT_UUID, Device.PRIMARY_ID, envelope).join();
    }

    final long expectedClearedBytes = (long) (cacheSize * EXTRA_ROOM_RATIO);

    final int persistedMessageCount = 100;
    final List<MessageProtos.Envelope> persistedMessages = new ArrayList<>(persistedMessageCount);
    final List<UUID> expectedClearedGuids = new ArrayList<>();
    long total = 0L;
    for (int i = 0; i < 100; i++) {
      final UUID guid = UUID.randomUUID();
      final MessageProtos.Envelope envelope = generateMessage(DESTINATION_ACCOUNT_UUID, guid, now.getEpochSecond(), 13);
      persistedMessages.add(envelope);
      if (total < expectedClearedBytes) {
        total += envelope.getSerializedSize();
        expectedClearedGuids.add(guid);
      }
    }

    setNextSlotToPersist(SlotHash.getSlot(queueName));

    final Device primary = mock(Device.class);
    when(primary.getId()).thenReturn((byte) 1);
    when(primary.isPrimary()).thenReturn(true);
    when(primary.getFetchesMessages()).thenReturn(true);
    when(destinationAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(primary));

    when(messagesManager.persistMessages(any(UUID.class), any(), anyList()))
        .thenThrow(ItemCollectionSizeLimitExceededException.builder().build());
    when(messagesManager.getMessagesForDeviceReactive(DESTINATION_ACCOUNT_UUID, primary, false))
        .thenReturn(Flux.concat(
            Flux.fromIterable(persistedMessages),
            Flux.fromIterable(cachedMessages)));
    when(messagesManager.delete(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
        messagePersister.persistNextQueues(Clock.systemUTC().instant()));

    verify(messagesManager, times(expectedClearedGuids.size()))
        .delete(eq(DESTINATION_ACCOUNT_UUID), eq(primary), argThat(expectedClearedGuids::contains), isNotNull());
    verify(messagesManager, never()).delete(any(), any(), argThat(guid -> !expectedClearedGuids.contains(guid)), any());

    final List<String> queuesToPersist = messagesCache.getQueuesToPersist(SlotHash.getSlot(queueName),
        Clock.systemUTC().instant(), 1);
    assertEquals(queuesToPersist.size(), 1);
    assertEquals(queuesToPersist.getFirst(), new String(queueName, StandardCharsets.UTF_8));
  }

  @Test
  void testFailedUnlinkOnFullQueueThrowsForRetry() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = 1;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    final Device primary = mock(Device.class);
    when(primary.getId()).thenReturn((byte) 1);
    when(primary.isPrimary()).thenReturn(true);
    when(primary.getFetchesMessages()).thenReturn(true);

    final Device activeA = mock(Device.class);
    when(activeA.getId()).thenReturn((byte) 2);
    when(activeA.getFetchesMessages()).thenReturn(true);

    final Device inactiveB = mock(Device.class);
    final byte inactiveId = 3;
    when(inactiveB.getId()).thenReturn(inactiveId);

    final Device inactiveC = mock(Device.class);
    when(inactiveC.getId()).thenReturn((byte) 4);

    final Device activeD = mock(Device.class);
    when(activeD.getId()).thenReturn((byte) 5);
    when(activeD.getFetchesMessages()).thenReturn(true);

    final Device destination = mock(Device.class);
    when(destination.getId()).thenReturn(DESTINATION_DEVICE_ID);

    when(destinationAccount.getDevices()).thenReturn(List.of(primary, activeA, inactiveB, inactiveC, activeD, destination));

    when(messagesManager.persistMessages(any(UUID.class), any(), anyList())).thenThrow(ItemCollectionSizeLimitExceededException.builder().build());
    when(accountsManager.removeDevice(destinationAccount, DESTINATION_DEVICE_ID)).thenReturn(CompletableFuture.failedFuture(new TimeoutException()));

    assertThrows(CompletionException.class, () -> messagePersister.persistQueue(destinationAccount, DESTINATION_DEVICE, "test"));
  }

  @SuppressWarnings("SameParameterValue")
  private static String generateRandomQueueNameForSlot(final int slot) {

    while (true) {

      final UUID uuid = UUID.randomUUID();
      final String queueNameBase = "user_queue::{" + uuid + "::";

      for (byte deviceId = 1; deviceId < Device.MAXIMUM_DEVICE_ID; deviceId++) {
        final String queueName = queueNameBase + deviceId + "}";

        if (SlotHash.getSlot(queueName) == slot) {
          return queueName;
        }
      }
    }
  }

  private void insertMessages(final UUID accountUuid, final byte deviceId, final int messageCount,
      final Instant firstMessageTimestamp) {
    for (int i = 0; i < messageCount; i++) {
      final UUID messageGuid = UUID.randomUUID();
      final MessageProtos.Envelope envelope = generateMessage(
          accountUuid, messageGuid, firstMessageTimestamp.toEpochMilli() + i, 256);
      messagesCache.insert(messageGuid, accountUuid, deviceId, envelope).join();
    }
  }

  private MessageProtos.Envelope generateMessage(UUID accountUuid, UUID messageGuid, long messageTimestamp, int contentSize) {
    return MessageProtos.Envelope.newBuilder()
        .setDestinationServiceId(accountUuid.toString())
        .setClientTimestamp(messageTimestamp)
        .setServerTimestamp(messageTimestamp)
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(contentSize)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(messageGuid.toString())
        .build();
  }

  private void setNextSlotToPersist(final int nextSlot) {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(
        connection -> connection.sync().set(MessagesCache.NEXT_SLOT_TO_PERSIST_KEY, String.valueOf(nextSlot - 1)));
  }
}
