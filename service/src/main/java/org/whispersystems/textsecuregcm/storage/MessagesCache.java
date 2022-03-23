/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;

public class MessagesCache extends RedisClusterPubSubAdapter<String, String> implements Managed {

  private final FaultTolerantRedisCluster readDeleteCluster;
  private final FaultTolerantPubSubConnection<String, String> pubSubConnection;

  private final ExecutorService notificationExecutorService;

  private final ClusterLuaScript insertScript;
  private final ClusterLuaScript removeByGuidScript;
  private final ClusterLuaScript getItemsScript;
  private final ClusterLuaScript removeQueueScript;
  private final ClusterLuaScript getQueuesToPersistScript;

  private final Map<String, MessageAvailabilityListener> messageListenersByQueueName = new HashMap<>();
  private final Map<MessageAvailabilityListener, String> queueNamesByMessageListener = new IdentityHashMap<>();

  private final Timer insertTimer = Metrics.timer(name(MessagesCache.class, "insert"));
  private final Timer getMessagesTimer = Metrics.timer(name(MessagesCache.class, "get"));
  private final Timer getQueuesToPersistTimer = Metrics.timer(name(MessagesCache.class, "getQueuesToPersist"));
  private final Timer clearQueueTimer = Metrics.timer(name(MessagesCache.class, "clear"));
  private final Counter pubSubMessageCounter = Metrics.counter(name(MessagesCache.class, "pubSubMessage"));
  private final Counter newMessageNotificationCounter = Metrics.counter(
      name(MessagesCache.class, "newMessageNotification"));
  private final Counter queuePersistedNotificationCounter = Metrics.counter(
      name(MessagesCache.class, "queuePersisted"));
  private final Counter staleEphemeralMessagesCounter = Metrics.counter(
      name(MessagesCache.class, "staleEphemeralMessages"));

  static final String NEXT_SLOT_TO_PERSIST_KEY = "user_queue_persist_slot";
  private static final byte[] LOCK_VALUE = "1".getBytes(StandardCharsets.UTF_8);

  private static final String QUEUE_KEYSPACE_PREFIX = "__keyspace@0__:user_queue::";
  private static final String PERSISTING_KEYSPACE_PREFIX = "__keyspace@0__:user_queue_persisting::";

  private static final Duration MAX_EPHEMERAL_MESSAGE_DELAY = Duration.ofSeconds(10);

  private static final String REMOVE_TIMER_NAME = name(MessagesCache.class, "remove");

  private static final String REMOVE_METHOD_TAG = "method";
  private static final String REMOVE_METHOD_UUID = "uuid";

  private static final Logger logger = LoggerFactory.getLogger(MessagesCache.class);

  public MessagesCache(final FaultTolerantRedisCluster insertCluster, final FaultTolerantRedisCluster readDeleteCluster,
      final ExecutorService notificationExecutorService) throws IOException {

    this.readDeleteCluster = readDeleteCluster;
    this.pubSubConnection = readDeleteCluster.createPubSubConnection();

    this.notificationExecutorService = notificationExecutorService;

    this.insertScript = ClusterLuaScript.fromResource(insertCluster, "lua/insert_item.lua", ScriptOutputType.INTEGER);
    this.removeByGuidScript = ClusterLuaScript.fromResource(readDeleteCluster, "lua/remove_item_by_guid.lua",
        ScriptOutputType.MULTI);
    this.getItemsScript = ClusterLuaScript.fromResource(readDeleteCluster, "lua/get_items.lua", ScriptOutputType.MULTI);
    this.removeQueueScript = ClusterLuaScript.fromResource(readDeleteCluster, "lua/remove_queue.lua",
        ScriptOutputType.STATUS);
    this.getQueuesToPersistScript = ClusterLuaScript.fromResource(readDeleteCluster, "lua/get_queues_to_persist.lua",
        ScriptOutputType.MULTI);
  }

  @Override
  public void start() {
    pubSubConnection.usePubSubConnection(connection -> {
      connection.addListener(this);
      connection.getResources().eventBus().get()
          .filter(event -> event instanceof ClusterTopologyChangedEvent)
          .subscribe(event -> resubscribeAll());
    });
  }

  @Override
  public void stop() {
    pubSubConnection.usePubSubConnection(connection -> connection.sync().upstream().commands().unsubscribe());
  }

  private void resubscribeAll() {
    logger.info("Got topology change event, resubscribing all keyspace notifications");

    final Set<String> queueNames;

    synchronized (messageListenersByQueueName) {
      queueNames = new HashSet<>(messageListenersByQueueName.keySet());
    }

    for (final String queueName : queueNames) {
      subscribeForKeyspaceNotifications(queueName);
    }
  }

  public long insert(final UUID guid, final UUID destinationUuid, final long destinationDevice,
      final MessageProtos.Envelope message) {
    final MessageProtos.Envelope messageWithGuid = message.toBuilder().setServerGuid(guid.toString()).build();
    return (long) insertTimer.record(() ->
        insertScript.executeBinary(List.of(getMessageQueueKey(destinationUuid, destinationDevice),
                getMessageQueueMetadataKey(destinationUuid, destinationDevice),
                getQueueIndexKey(destinationUuid, destinationDevice)),
            List.of(messageWithGuid.toByteArray(),
                String.valueOf(message.getTimestamp()).getBytes(StandardCharsets.UTF_8),
                guid.toString().getBytes(StandardCharsets.UTF_8))));
  }

  public Optional<OutgoingMessageEntity> remove(final UUID destinationUuid, final long destinationDevice,
      final UUID messageGuid) {
    return remove(destinationUuid, destinationDevice, List.of(messageGuid)).stream().findFirst();
  }

  @SuppressWarnings("unchecked")
  public List<OutgoingMessageEntity> remove(final UUID destinationUuid, final long destinationDevice,
      final List<UUID> messageGuids) {
    final List<byte[]> serialized = (List<byte[]>) Metrics.timer(REMOVE_TIMER_NAME, REMOVE_METHOD_TAG,
        REMOVE_METHOD_UUID).record(() ->
        removeByGuidScript.executeBinary(List.of(getMessageQueueKey(destinationUuid, destinationDevice),
                getMessageQueueMetadataKey(destinationUuid, destinationDevice),
                getQueueIndexKey(destinationUuid, destinationDevice)),
            messageGuids.stream().map(guid -> guid.toString().getBytes(StandardCharsets.UTF_8))
                .collect(Collectors.toList())));

    final List<OutgoingMessageEntity> removedMessages = new ArrayList<>(serialized.size());

    for (final byte[] bytes : serialized) {
      try {
        removedMessages.add(constructEntityFromEnvelope(MessageProtos.Envelope.parseFrom(bytes)));
      } catch (final InvalidProtocolBufferException e) {
        logger.warn("Failed to parse envelope", e);
      }
    }

    return removedMessages;
  }

  public boolean hasMessages(final UUID destinationUuid, final long destinationDevice) {
    return readDeleteCluster.withBinaryCluster(
        connection -> connection.sync().zcard(getMessageQueueKey(destinationUuid, destinationDevice)) > 0);
  }

  @SuppressWarnings("unchecked")
  public List<OutgoingMessageEntity> get(final UUID destinationUuid, final long destinationDevice, final int limit) {
    return getMessagesTimer.record(() -> {
      final List<byte[]> queueItems = (List<byte[]>) getItemsScript.executeBinary(
          List.of(getMessageQueueKey(destinationUuid, destinationDevice),
              getPersistInProgressKey(destinationUuid, destinationDevice)),
          List.of(String.valueOf(limit).getBytes(StandardCharsets.UTF_8)));

      final long earliestAllowableEphemeralTimestamp =
          System.currentTimeMillis() - MAX_EPHEMERAL_MESSAGE_DELAY.toMillis();

      final List<OutgoingMessageEntity> messageEntities;
      final List<UUID> staleEphemeralMessageGuids = new ArrayList<>();

      if (queueItems.size() % 2 == 0) {
        messageEntities = new ArrayList<>(queueItems.size() / 2);

        for (int i = 0; i < queueItems.size() - 1; i += 2) {
          try {
            final MessageProtos.Envelope message = MessageProtos.Envelope.parseFrom(queueItems.get(i));
            if (message.getEphemeral() && message.getTimestamp() < earliestAllowableEphemeralTimestamp) {
              staleEphemeralMessageGuids.add(UUID.fromString(message.getServerGuid()));
              continue;
            }

            final long id = Long.parseLong(new String(queueItems.get(i + 1), StandardCharsets.UTF_8));

            messageEntities.add(constructEntityFromEnvelope(message));
          } catch (InvalidProtocolBufferException e) {
            logger.warn("Failed to parse envelope", e);
          }
        }
      } else {
        logger.error("\"Get messages\" operation returned a list with a non-even number of elements.");
        messageEntities = Collections.emptyList();
      }

      try {
        remove(destinationUuid, destinationDevice, staleEphemeralMessageGuids);
        staleEphemeralMessagesCounter.increment(staleEphemeralMessageGuids.size());
      } catch (final Throwable e) {
        logger.warn("Could not remove stale ephemeral messages from cache", e);
      }

      return messageEntities;
    });
  }

  @VisibleForTesting
  List<MessageProtos.Envelope> getMessagesToPersist(final UUID accountUuid, final long destinationDevice,
      final int limit) {
    return getMessagesTimer.record(() -> {
      final List<ScoredValue<byte[]>> scoredMessages = readDeleteCluster.withBinaryCluster(
          connection -> connection.sync()
              .zrangeWithScores(getMessageQueueKey(accountUuid, destinationDevice), 0, limit));
      final List<MessageProtos.Envelope> envelopes = new ArrayList<>(scoredMessages.size());

      for (final ScoredValue<byte[]> scoredMessage : scoredMessages) {
        try {
          envelopes.add(MessageProtos.Envelope.parseFrom(scoredMessage.getValue()));
        } catch (InvalidProtocolBufferException e) {
          logger.warn("Failed to parse envelope", e);
        }
      }

      return envelopes;
    });
  }

  public void clear(final UUID destinationUuid) {
    // TODO Remove null check in a fully UUID-based world
    if (destinationUuid != null) {
      for (int i = 1; i < 256; i++) {
        clear(destinationUuid, i);
      }
    }
  }

  public void clear(final UUID destinationUuid, final long deviceId) {
    clearQueueTimer.record(() ->
        removeQueueScript.executeBinary(List.of(getMessageQueueKey(destinationUuid, deviceId),
                getMessageQueueMetadataKey(destinationUuid, deviceId),
                getQueueIndexKey(destinationUuid, deviceId)),
            Collections.emptyList()));
  }

  int getNextSlotToPersist() {
    return (int) (readDeleteCluster.withCluster(connection -> connection.sync().incr(NEXT_SLOT_TO_PERSIST_KEY))
        % SlotHash.SLOT_COUNT);
  }

  List<String> getQueuesToPersist(final int slot, final Instant maxTime, final int limit) {
    //noinspection unchecked
    return getQueuesToPersistTimer.record(() -> (List<String>) getQueuesToPersistScript.execute(
        List.of(new String(getQueueIndexKey(slot), StandardCharsets.UTF_8)),
        List.of(String.valueOf(maxTime.toEpochMilli()),
            String.valueOf(limit))));
  }

  void addQueueToPersist(final UUID accountUuid, final long deviceId) {
    readDeleteCluster.useBinaryCluster(connection -> connection.sync()
        .zadd(getQueueIndexKey(accountUuid, deviceId), ZAddArgs.Builder.nx(), System.currentTimeMillis(),
            getMessageQueueKey(accountUuid, deviceId)));
  }

  void lockQueueForPersistence(final UUID accountUuid, final long deviceId) {
    readDeleteCluster.useBinaryCluster(
        connection -> connection.sync().setex(getPersistInProgressKey(accountUuid, deviceId), 30, LOCK_VALUE));
  }

  void unlockQueueForPersistence(final UUID accountUuid, final long deviceId) {
    readDeleteCluster.useBinaryCluster(
        connection -> connection.sync().del(getPersistInProgressKey(accountUuid, deviceId)));
  }

  public void addMessageAvailabilityListener(final UUID destinationUuid, final long deviceId,
      final MessageAvailabilityListener listener) {
    final String queueName = getQueueName(destinationUuid, deviceId);

    synchronized (messageListenersByQueueName) {
      messageListenersByQueueName.put(queueName, listener);
      queueNamesByMessageListener.put(listener, queueName);
    }

    subscribeForKeyspaceNotifications(queueName);
  }

  public void removeMessageAvailabilityListener(final MessageAvailabilityListener listener) {
    final String queueName = queueNamesByMessageListener.remove(listener);

    unsubscribeFromKeyspaceNotifications(queueName);

    synchronized (messageListenersByQueueName) {
      if (queueName != null) {
        messageListenersByQueueName.remove(queueName);
      }
    }
  }

  private void subscribeForKeyspaceNotifications(final String queueName) {
    final int slot = SlotHash.getSlot(queueName);

    pubSubConnection.usePubSubConnection(
        connection -> connection.sync().nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(slot))
            .commands()
            .subscribe(getKeyspaceChannels(queueName)));
  }

  private void unsubscribeFromKeyspaceNotifications(final String queueName) {
    pubSubConnection.usePubSubConnection(connection -> connection.sync().upstream()
        .commands()
        .unsubscribe(getKeyspaceChannels(queueName)));
  }

  private static String[] getKeyspaceChannels(final String queueName) {
    return new String[]{
        QUEUE_KEYSPACE_PREFIX + "{" + queueName + "}",
        PERSISTING_KEYSPACE_PREFIX + "{" + queueName + "}"
    };
  }

  @Override
  public void message(final RedisClusterNode node, final String channel, final String message) {
    pubSubMessageCounter.increment();

    if (channel.startsWith(QUEUE_KEYSPACE_PREFIX) && "zadd".equals(message)) {
      newMessageNotificationCounter.increment();
      notificationExecutorService.execute(() -> {
        try {
          findListener(channel).ifPresent(MessageAvailabilityListener::handleNewMessagesAvailable);
        } catch (final Exception e) {
          logger.warn("Unexpected error handling new message", e);
        }
      });
    } else if (channel.startsWith(PERSISTING_KEYSPACE_PREFIX) && "del".equals(message)) {
      queuePersistedNotificationCounter.increment();
      notificationExecutorService.execute(() -> {
        try {
          findListener(channel).ifPresent(MessageAvailabilityListener::handleMessagesPersisted);
        } catch (final Exception e) {
          logger.warn("Unexpected error handling messages persisted", e);
        }
      });
    }
  }

  private Optional<MessageAvailabilityListener> findListener(final String keyspaceChannel) {
    final String queueName = getQueueNameFromKeyspaceChannel(keyspaceChannel);

    synchronized (messageListenersByQueueName) {
      return Optional.ofNullable(messageListenersByQueueName.get(queueName));
    }
  }

  @VisibleForTesting
  static OutgoingMessageEntity constructEntityFromEnvelope(MessageProtos.Envelope envelope) {
    return new OutgoingMessageEntity(
        envelope.hasServerGuid() ? UUID.fromString(envelope.getServerGuid()) : null,
        envelope.getType().getNumber(),
        envelope.getTimestamp(),
        envelope.getSource(),
        envelope.hasSourceUuid() ? UUID.fromString(envelope.getSourceUuid()) : null,
        envelope.getSourceDevice(),
        envelope.hasDestinationUuid() ? UUID.fromString(envelope.getDestinationUuid()) : null,
        envelope.hasContent() ? envelope.getContent().toByteArray() : null,
        envelope.hasServerTimestamp() ? envelope.getServerTimestamp() : 0);
  }

  @VisibleForTesting
  static String getQueueName(final UUID accountUuid, final long deviceId) {
    return accountUuid + "::" + deviceId;
  }

  @VisibleForTesting
  static String getQueueNameFromKeyspaceChannel(final String channel) {
    final int startOfHashTag = channel.indexOf('{');
    final int endOfHashTag = channel.lastIndexOf('}');

    return channel.substring(startOfHashTag + 1, endOfHashTag);
  }

  @VisibleForTesting
  static byte[] getMessageQueueKey(final UUID accountUuid, final long deviceId) {
    return ("user_queue::{" + accountUuid.toString() + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] getMessageQueueMetadataKey(final UUID accountUuid, final long deviceId) {
    return ("user_queue_metadata::{" + accountUuid.toString() + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] getQueueIndexKey(final UUID accountUuid, final long deviceId) {
    return getQueueIndexKey(SlotHash.getSlot(accountUuid.toString() + "::" + deviceId));
  }

  private static byte[] getQueueIndexKey(final int slot) {
    return ("user_queue_index::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}").getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] getPersistInProgressKey(final UUID accountUuid, final long deviceId) {
    return ("user_queue_persisting::{" + accountUuid + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  static UUID getAccountUuidFromQueueName(final String queueName) {
    final int startOfHashTag = queueName.indexOf('{');

    return UUID.fromString(queueName.substring(startOfHashTag + 1, queueName.indexOf("::", startOfHashTag)));
  }

  static long getDeviceIdFromQueueName(final String queueName) {
    return Long.parseLong(queueName.substring(queueName.lastIndexOf("::") + 2, queueName.lastIndexOf('}')));
  }
}
