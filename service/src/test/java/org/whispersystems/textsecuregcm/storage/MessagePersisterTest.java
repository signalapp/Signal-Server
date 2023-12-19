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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.MockUtils.exactly;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.lettuce.core.cluster.SlotHash;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.model.ItemCollectionSizeLimitExceededException;

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
  private ClientPresenceManager clientPresenceManager;
  private KeysManager keysManager;
  private MessagesManager messagesManager;
  private Account destinationAccount;

  private static final UUID DESTINATION_ACCOUNT_UUID = UUID.randomUUID();
  private static final String DESTINATION_ACCOUNT_NUMBER = "+18005551234";
  private static final byte DESTINATION_DEVICE_ID = 7;

  private static final Duration PERSIST_DELAY = Duration.ofMinutes(5);

  @BeforeEach
  void setUp() throws Exception {

    messagesManager = mock(MessagesManager.class);
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(
        DynamicConfigurationManager.class);

    messagesDynamoDb = mock(MessagesDynamoDb.class);
    accountsManager = mock(AccountsManager.class);
    clientPresenceManager = mock(ClientPresenceManager.class);
    keysManager = mock(KeysManager.class);
    destinationAccount = mock(Account.class);;

    when(accountsManager.getByAccountIdentifier(DESTINATION_ACCOUNT_UUID)).thenReturn(Optional.of(destinationAccount));
    when(accountsManager.removeDevice(any(), anyByte()))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(invocation.getArgument(0)));

    when(destinationAccount.getUuid()).thenReturn(DESTINATION_ACCOUNT_UUID);
    when(destinationAccount.getNumber()).thenReturn(DESTINATION_ACCOUNT_NUMBER);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    sharedExecutorService = Executors.newSingleThreadExecutor();
    resubscribeRetryExecutorService = Executors.newSingleThreadScheduledExecutor();
    messageDeliveryScheduler = Schedulers.newBoundedElastic(10, 10_000, "messageDelivery");
    messagesCache = new MessagesCache(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        REDIS_CLUSTER_EXTENSION.getRedisCluster(), sharedExecutorService, messageDeliveryScheduler,
        sharedExecutorService, Clock.systemUTC());
    messagePersister = new MessagePersister(messagesCache, messagesManager, accountsManager, clientPresenceManager,
        keysManager, dynamicConfigurationManager, PERSIST_DELAY, 1, MoreExecutors.newDirectExecutorService());

    when(messagesManager.persistMessages(any(UUID.class), anyByte(), any())).thenAnswer(invocation -> {
      final UUID destinationUuid = invocation.getArgument(0);
      final byte destinationDeviceId = invocation.getArgument(1);
      final List<MessageProtos.Envelope> messages = invocation.getArgument(2);

      messagesDynamoDb.store(messages, destinationUuid, destinationDeviceId);

      for (final MessageProtos.Envelope message : messages) {
        messagesCache.remove(destinationUuid, destinationDeviceId, UUID.fromString(message.getServerGuid())).get();
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
        eq(DESTINATION_DEVICE_ID));
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

    verify(messagesDynamoDb, never()).store(any(), any(), anyByte());
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

      insertMessages(accountUuid, deviceId, messagesPerQueue, now);
    }

    setNextSlotToPersist(slot);

    messagePersister.persistNextQueues(now.plus(messagePersister.getPersistDelay()));

    final ArgumentCaptor<List<MessageProtos.Envelope>> messagesCaptor = ArgumentCaptor.forClass(List.class);

    verify(messagesDynamoDb, atLeastOnce()).store(messagesCaptor.capture(), any(UUID.class), anyByte());
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
    }).when(messagesDynamoDb).store(any(), eq(DESTINATION_ACCOUNT_UUID), eq(DESTINATION_DEVICE_ID));

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
    when(messagesManager.persistMessages(any(UUID.class), anyByte(), anyList())).thenReturn(0);

    assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
        assertThrows(MessagePersistenceException.class,
            () -> messagePersister.persistQueue(destinationAccount, DESTINATION_DEVICE_ID)));
  }

  @Test
  void testUnlinkFirstInactiveDeviceOnFullQueue() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = 1;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    final Device primary = mock(Device.class);
    when(primary.getId()).thenReturn((byte) 1);
    when(primary.isPrimary()).thenReturn(true);
    when(primary.isEnabled()).thenReturn(true);
    final Device activeA = mock(Device.class);
    when(activeA.getId()).thenReturn((byte) 2);
    when(activeA.isEnabled()).thenReturn(true);
    final Device inactiveB = mock(Device.class);
    final byte inactiveId = 3;
    when(inactiveB.getId()).thenReturn(inactiveId);
    when(inactiveB.isEnabled()).thenReturn(false);
    final Device inactiveC = mock(Device.class);
    when(inactiveC.getId()).thenReturn((byte) 4);
    when(inactiveC.isEnabled()).thenReturn(false);
    final Device activeD = mock(Device.class);
    when(activeD.getId()).thenReturn((byte) 5);
    when(activeD.isEnabled()).thenReturn(true);
    final Device destination = mock(Device.class);
    when(destination.getId()).thenReturn(DESTINATION_DEVICE_ID);
    when(destination.isEnabled()).thenReturn(true);

    when(destinationAccount.getDevices()).thenReturn(List.of(primary, activeA, inactiveB, inactiveC, activeD, destination));

    when(messagesManager.persistMessages(any(UUID.class), anyByte(), anyList())).thenThrow(ItemCollectionSizeLimitExceededException.builder().build());
    when(messagesManager.clear(any(UUID.class), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.deleteSingleUsePreKeys(any(), eq(inactiveId))).thenReturn(CompletableFuture.completedFuture(null));

    assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
        messagePersister.persistQueue(destinationAccount, DESTINATION_DEVICE_ID));

    verify(messagesManager, exactly()).clear(DESTINATION_ACCOUNT_UUID, inactiveId);
  }

  @Test
  void testUnlinkActiveDeviceWithOldestMessageOnFullQueueWithNoInactiveDevices() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = 1;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    final Device primary = mock(Device.class);
    final byte primaryId = 1;
    when(primary.getId()).thenReturn(primaryId);
    when(primary.isPrimary()).thenReturn(true);
    when(primary.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(primaryId)))
        .thenReturn(Mono.just(4L));

    final Device deviceA = mock(Device.class);
    final byte deviceIdA = 2;
    when(deviceA.getId()).thenReturn(deviceIdA);
    when(deviceA.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(deviceIdA)))
        .thenReturn(Mono.empty());

    final Device deviceB = mock(Device.class);
    final byte deviceIdB = 3;
    when(deviceB.getId()).thenReturn(deviceIdB);
    when(deviceB.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(deviceIdB)))
        .thenReturn(Mono.just(2L));

    final Device destination = mock(Device.class);
    when(destination.getId()).thenReturn(DESTINATION_DEVICE_ID);
    when(destination.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(DESTINATION_DEVICE_ID)))
        .thenReturn(Mono.just(5L));

    when(destinationAccount.getDevices()).thenReturn(List.of(primary, deviceA, deviceB, destination));

    when(messagesManager.persistMessages(any(UUID.class), anyByte(), anyList())).thenThrow(ItemCollectionSizeLimitExceededException.builder().build());
    when(messagesManager.clear(any(UUID.class), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.deleteSingleUsePreKeys(any(), eq(deviceIdB))).thenReturn(CompletableFuture.completedFuture(null));

    assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
        messagePersister.persistQueue(destinationAccount, DESTINATION_DEVICE_ID));

    verify(messagesManager, exactly()).clear(DESTINATION_ACCOUNT_UUID, deviceIdB);
  }

  @Test
  void testUnlinkDestinationDevice() {
    final String queueName = new String(
        MessagesCache.getMessageQueueKey(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID), StandardCharsets.UTF_8);
    final int messageCount = 1;
    final Instant now = Instant.now();

    insertMessages(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID, messageCount, now);
    setNextSlotToPersist(SlotHash.getSlot(queueName));

    final Device primary = mock(Device.class);
    final byte primaryId = 1;
    when(primary.getId()).thenReturn(primaryId);
    when(primary.isPrimary()).thenReturn(true);
    when(primary.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(primaryId)))
        .thenReturn(Mono.just(1L));

    final Device deviceA = mock(Device.class);
    final byte deviceIdA = 2;
    when(deviceA.getId()).thenReturn(deviceIdA);
    when(deviceA.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(deviceIdA)))
        .thenReturn(Mono.just(3L));

    final Device deviceB = mock(Device.class);
    final byte deviceIdB = 2;
    when(deviceB.getId()).thenReturn(deviceIdB);
    when(deviceB.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(deviceIdB)))
        .thenReturn(Mono.empty());

    final Device destination = mock(Device.class);
    when(destination.getId()).thenReturn(DESTINATION_DEVICE_ID);
    when(destination.isEnabled()).thenReturn(true);
    when(messagesManager.getEarliestUndeliveredTimestampForDevice(any(), eq(DESTINATION_DEVICE_ID)))
        .thenReturn(Mono.just(2L));

    when(destinationAccount.getDevices()).thenReturn(List.of(primary, deviceA, deviceB, destination));

    when(messagesManager.persistMessages(any(UUID.class), anyByte(), anyList())).thenThrow(ItemCollectionSizeLimitExceededException.builder().build());
    when(messagesManager.clear(any(UUID.class), anyByte())).thenReturn(CompletableFuture.completedFuture(null));
    when(keysManager.deleteSingleUsePreKeys(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(null));

    assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
        messagePersister.persistQueue(destinationAccount, DESTINATION_DEVICE_ID));

    verify(messagesManager, exactly()).clear(DESTINATION_ACCOUNT_UUID, DESTINATION_DEVICE_ID);
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

      final MessageProtos.Envelope envelope = MessageProtos.Envelope.newBuilder()
          .setTimestamp(firstMessageTimestamp.toEpochMilli() + i)
          .setServerTimestamp(firstMessageTimestamp.toEpochMilli() + i)
          .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
          .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
          .setServerGuid(messageGuid.toString())
          .build();

      messagesCache.insert(messageGuid, accountUuid, deviceId, envelope);
    }
  }

  private void setNextSlotToPersist(final int nextSlot) {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(
        connection -> connection.sync().set(MessagesCache.NEXT_SLOT_TO_PERSIST_KEY, String.valueOf(nextSlot - 1)));
  }
}
