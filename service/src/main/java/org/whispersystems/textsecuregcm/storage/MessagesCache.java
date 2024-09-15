/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.ServiceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessagesConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.Experiment;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Manages short-term storage of messages in Redis. Messages are frequently delivered to their destination and deleted
 * shortly after they reach the server, and this cache acts as a low-latency holding area for new messages, reducing
 * load on higher-latency, longer-term storage systems. Redis in particular provides keyspace notifications, which act
 * as a form of pub-sub notifications to alert listeners when new messages arrive.
 * <p>
 * The following structures are used:
 * <dl>
 *   <dt>{@code queueKey}</code></dt>
 *   <dd>A sorted set of messages in a device’s queue. A message’s score is its queue-local message ID. See
 *   <a href="https://redis.io/docs/latest/develop/use/patterns/twitter-clone/#the-sorted-set-data-type">Redis.io: The
 *   Sorted Set data type</a> for background on scores and this data structure.</dd>
 *   <dt>{@code queueMetadataKey}</dt>
 *   <dd>A hash containing message guids and their queue-local message ID. It also contains a {@code counter} key, which is
 *   incremented to supply the next message ID. This is used to remove a message by GUID from {@code queueKey} by its
 *   local messageId.</dd>
 *   <dt>{@code sharedMrmKey}</dt>
 *   <dd>A hash containing a single multi-recipient message pending delivery. It contains:
 *     <ul>
 *       <li>{@code data} - the serialized SealedSenderMultiRecipientMessage data</li>
 *       <li>fields with each recipient device's “view” into the payload ({@link SealedSenderMultiRecipientMessage#serializedRecipientView(SealedSenderMultiRecipientMessage.Recipient)}</li>
 *     </ul>
 *     Note: this is shared among all of the message's recipients, and it may be located in any Redis shard. As each recipient’s
 *     message is delivered, its corresponding view is idempotently removed. When {@code data} is the only remaining
 *     field, the hash will be deleted.
 *     </dd>
 *   <dt>{@code queueLockKey}</dt>
 *   <dd>Used to indicate that a queue is being modified by the {@link MessagePersister} and that {@code get_items} should
 *   return an empty list.</dd>
 *   <dt>{@code queueTotalIndexKey}</dt>
 *   <dd>A sorted set of all queues in a shard. A queue’s score is the timestamp of its oldest message, which is used by
 *   the {@link MessagePersister} to prioritize queues to persist.</dd>
 * </dl>
 * <p>
 * At a high level, the process is:
 * <ol>
 *   <li>Insert: the queue metadata is queried for the next incremented message ID. The message data is inserted into
 *   the queue at that ID, and the message GUID is inserted in the queue metadata.</li>
 *   <li>Get: a batch of messages are retrieved from the queue, potentially with an after-message-ID offset.</li>
 *   <li>Remove: a set of messages are remove by GUID. For each GUID, the message ID is retrieved from the queue metadata,
 *   and then that single-value range is removed from the queue.</li>
 * </ol>
 * For multi-recipient messages (sometimes abbreviated “MRM”), there are similar operations on the common data during
 * insert, get, and remove. MRM inserts must occur before individual queue inserts, while removal is considered
 * best-effort, and uses key expiration as back-stop garbage collection.
 * <p>
 * For atomicity, many operations are implemented as Lua scripts that are executed on the Redis server using
 * {@code EVAL}/{@code EVALSHA}.
 *
 * @see MessagesCacheInsertScript
 * @see MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript
 * @see MessagesCacheGetItemsScript
 * @see MessagesCacheRemoveByGuidScript
 * @see MessagesCacheRemoveRecipientViewFromMrmDataScript
 * @see MessagesCacheRemoveQueueScript
 */
public class MessagesCache extends RedisClusterPubSubAdapter<String, String> implements Managed {

  private final FaultTolerantRedisCluster redisCluster;
  private final FaultTolerantPubSubConnection<String, String> pubSubConnection;
  private final Clock clock;

  private final ExecutorService notificationExecutorService;
  private final Scheduler messageDeliveryScheduler;
  private final ExecutorService messageDeletionExecutorService;
  // messageDeletionExecutorService wrapped into a reactor Scheduler
  private final Scheduler messageDeletionScheduler;

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private final MessagesCacheInsertScript insertScript;
  private final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript;
  private final MessagesCacheRemoveByGuidScript removeByGuidScript;
  private final MessagesCacheGetItemsScript getItemsScript;
  private final MessagesCacheRemoveQueueScript removeQueueScript;
  private final MessagesCacheGetQueuesToPersistScript getQueuesToPersistScript;
  private final MessagesCacheRemoveRecipientViewFromMrmDataScript removeRecipientViewFromMrmDataScript;

  private final ReentrantLock messageListenersLock = new ReentrantLock();
  private final Map<String, MessageAvailabilityListener> messageListenersByQueueName = new HashMap<>();
  private final Map<MessageAvailabilityListener, String> queueNamesByMessageListener = new IdentityHashMap<>();

  private final Timer insertTimer = Metrics.timer(name(MessagesCache.class, "insert"));
  private final Timer insertSharedMrmPayloadTimer = Metrics.timer(name(MessagesCache.class, "insertSharedMrmPayload"));
  private final Timer getMessagesTimer = Metrics.timer(name(MessagesCache.class, "get"));
  private final Timer getQueuesToPersistTimer = Metrics.timer(name(MessagesCache.class, "getQueuesToPersist"));
  private final Timer removeByGuidTimer = Metrics.timer(name(MessagesCache.class, "removeByGuid"));
  private final Timer removeRecipientViewTimer = Metrics.timer(name(MessagesCache.class, "removeRecipientView"));
  private final Timer clearQueueTimer = Metrics.timer(name(MessagesCache.class, "clear"));
  private final Counter pubSubMessageCounter = Metrics.counter(name(MessagesCache.class, "pubSubMessage"));
  private final Counter newMessageNotificationCounter = Metrics.counter(
      name(MessagesCache.class, "newMessageNotification"));
  private final Counter queuePersistedNotificationCounter = Metrics.counter(
      name(MessagesCache.class, "queuePersisted"));
  private final Counter staleEphemeralMessagesCounter = Metrics.counter(
      name(MessagesCache.class, "staleEphemeralMessages"));
  private final Counter messageAvailabilityListenerRemovedAfterAddCounter = Metrics.counter(
      name(MessagesCache.class, "messageAvailabilityListenerRemovedAfterAdd"));
  private final Counter prunedStaleSubscriptionCounter = Metrics.counter(
      name(MessagesCache.class, "prunedStaleSubscription"));
  private final Counter mrmContentRetrievedCounter = Metrics.counter(name(MessagesCache.class, "mrmViewRetrieved"));
  private final Counter sharedMrmDataKeyRemovedCounter = Metrics.counter(
      name(MessagesCache.class, "sharedMrmKeyRemoved"));

  static final String NEXT_SLOT_TO_PERSIST_KEY = "user_queue_persist_slot";
  private static final byte[] LOCK_VALUE = "1".getBytes(StandardCharsets.UTF_8);

  private static final String QUEUE_KEYSPACE_PREFIX = "__keyspace@0__:user_queue::";
  private static final String PERSISTING_KEYSPACE_PREFIX = "__keyspace@0__:user_queue_persisting::";

  private static final String MRM_VIEWS_EXPERIMENT_NAME = "mrmViews";

  @VisibleForTesting
  static final Duration MAX_EPHEMERAL_MESSAGE_DELAY = Duration.ofSeconds(10);

  private static final String GET_FLUX_NAME = MetricsUtil.name(MessagesCache.class, "get");
  private static final int PAGE_SIZE = 100;

  private static final int REMOVE_MRM_RECIPIENT_VIEW_CONCURRENCY = 8;

  private static final Logger logger = LoggerFactory.getLogger(MessagesCache.class);

  public MessagesCache(final FaultTolerantRedisCluster redisCluster, final ExecutorService notificationExecutorService,
      final Scheduler messageDeliveryScheduler, final ExecutorService messageDeletionExecutorService, final Clock clock,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager)
      throws IOException {
    this(
        redisCluster,
        notificationExecutorService,
        messageDeliveryScheduler,
        messageDeletionExecutorService,
        clock,
        dynamicConfigurationManager,
        new MessagesCacheInsertScript(redisCluster),
        new MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(redisCluster),
        new MessagesCacheGetItemsScript(redisCluster),
        new MessagesCacheRemoveByGuidScript(redisCluster),
        new MessagesCacheRemoveQueueScript(redisCluster),
        new MessagesCacheGetQueuesToPersistScript(redisCluster),
        new MessagesCacheRemoveRecipientViewFromMrmDataScript(redisCluster)
    );
  }

  @VisibleForTesting
  MessagesCache(final FaultTolerantRedisCluster redisCluster, final ExecutorService notificationExecutorService,
      final Scheduler messageDeliveryScheduler, final ExecutorService messageDeletionExecutorService, final Clock clock,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final MessagesCacheInsertScript insertScript,
      final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript,
      final MessagesCacheGetItemsScript getItemsScript, final MessagesCacheRemoveByGuidScript removeByGuidScript,
      final MessagesCacheRemoveQueueScript removeQueueScript,
      final MessagesCacheGetQueuesToPersistScript getQueuesToPersistScript,
      final MessagesCacheRemoveRecipientViewFromMrmDataScript removeRecipientViewFromMrmDataScript)
      throws IOException {

    this.redisCluster = redisCluster;
    this.pubSubConnection = redisCluster.createPubSubConnection();
    this.clock = clock;

    this.notificationExecutorService = notificationExecutorService;
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.messageDeletionExecutorService = messageDeletionExecutorService;
    this.messageDeletionScheduler = Schedulers.fromExecutorService(messageDeletionExecutorService, "messageDeletion");

    this.dynamicConfigurationManager = dynamicConfigurationManager;

    this.insertScript = insertScript;
    this.insertMrmScript = insertMrmScript;
    this.removeByGuidScript = removeByGuidScript;
    this.getItemsScript = getItemsScript;
    this.removeQueueScript = removeQueueScript;
    this.getQueuesToPersistScript = getQueuesToPersistScript;
    this.removeRecipientViewFromMrmDataScript = removeRecipientViewFromMrmDataScript;
  }

  @Override
  public void start() {
    pubSubConnection.usePubSubConnection(connection -> connection.addListener(this));
    pubSubConnection.subscribeToClusterTopologyChangedEvents(this::resubscribeAll);
  }

  @Override
  public void stop() {
    pubSubConnection.usePubSubConnection(connection -> connection.sync().upstream().commands().unsubscribe());
  }

  private void resubscribeAll() {

    final Set<String> queueNames;

    messageListenersLock.lock();
    try {
      queueNames = new HashSet<>(messageListenersByQueueName.keySet());
    } finally {
      messageListenersLock.unlock();
    }

    for (final String queueName : queueNames) {
      // avoid overwhelming a newly recovered node by processing synchronously, rather than using CompletableFuture.allOf()
      subscribeForKeyspaceNotifications(queueName).join();
    }
  }

  public long insert(final UUID guid, final UUID destinationUuid, final byte destinationDevice,
      final MessageProtos.Envelope message) {
    final MessageProtos.Envelope messageWithGuid = message.toBuilder().setServerGuid(guid.toString()).build();
    return insertTimer.record(() -> insertScript.execute(destinationUuid, destinationDevice, messageWithGuid));
  }

  public byte[] insertSharedMultiRecipientMessagePayload(
      final SealedSenderMultiRecipientMessage sealedSenderMultiRecipientMessage) {
    return insertSharedMrmPayloadTimer.record(() -> {
          final byte[] sharedMrmKey = getSharedMrmKey(UUID.randomUUID());
          insertMrmScript.execute(sharedMrmKey, sealedSenderMultiRecipientMessage);
          return sharedMrmKey;
      });
  }

  public CompletableFuture<Optional<RemovedMessage>> remove(final UUID destinationUuid, final byte destinationDevice,
      final UUID messageGuid) {

    return remove(destinationUuid, destinationDevice, List.of(messageGuid))
        .thenApply(removed -> removed.isEmpty() ? Optional.empty() : Optional.of(removed.getFirst()));
  }

  public CompletableFuture<List<RemovedMessage>> remove(final UUID destinationUuid, final byte destinationDevice,
      final List<UUID> messageGuids) {

    final Timer.Sample sample = Timer.start();

    return removeByGuidScript.execute(destinationUuid, destinationDevice, messageGuids)
        .thenApplyAsync(serialized -> {

          final List<RemovedMessage> removedMessages = new ArrayList<>(serialized.size());
          final List<byte[]> sharedMrmKeysToUpdate = new ArrayList<>();

          for (final byte[] bytes : serialized) {
            try {
              final MessageProtos.Envelope envelope = MessageProtos.Envelope.parseFrom(bytes);
              removedMessages.add(RemovedMessage.fromEnvelope(envelope));
              if (envelope.hasSharedMrmKey()) {
                sharedMrmKeysToUpdate.add(envelope.getSharedMrmKey().toByteArray());
              }
            } catch (final InvalidProtocolBufferException e) {
              logger.warn("Failed to parse envelope", e);
            }
          }

          removeRecipientViewFromMrmData(sharedMrmKeysToUpdate, destinationUuid, destinationDevice);
          return removedMessages;
        }, messageDeletionExecutorService).whenComplete((ignored, throwable) -> sample.stop(removeByGuidTimer));

  }

  public boolean hasMessages(final UUID destinationUuid, final byte destinationDevice) {
    return redisCluster.withBinaryCluster(
        connection -> connection.sync().zcard(getMessageQueueKey(destinationUuid, destinationDevice)) > 0);
  }

  public CompletableFuture<Boolean> hasMessagesAsync(final UUID destinationUuid, final byte destinationDevice) {
    return redisCluster.withBinaryCluster(connection ->
            connection.async().zcard(getMessageQueueKey(destinationUuid, destinationDevice))
                .thenApply(cardinality -> cardinality > 0))
        .toCompletableFuture();
  }

  public Publisher<MessageProtos.Envelope> get(final UUID destinationUuid, final byte destinationDevice) {

    final long earliestAllowableEphemeralTimestamp =
        clock.millis() - MAX_EPHEMERAL_MESSAGE_DELAY.toMillis();

    final Flux<MessageProtos.Envelope> allMessages = getAllMessages(destinationUuid, destinationDevice)
        .publish()
        // We expect exactly two subscribers to this base flux:
        // 1. the websocket that delivers messages to clients
        // 2. an internal process to discard stale ephemeral messages
        // The discard subscriber will subscribe immediately, but we don’t want to do any work if the
        // websocket never subscribes.
        .autoConnect(2);

    final Flux<MessageProtos.Envelope> messagesToPublish = allMessages
        .filter(Predicate.not(envelope -> isStaleEphemeralMessage(envelope, earliestAllowableEphemeralTimestamp)));

    final Flux<MessageProtos.Envelope> staleEphemeralMessages = allMessages
        .filter(envelope -> isStaleEphemeralMessage(envelope, earliestAllowableEphemeralTimestamp));

    discardStaleEphemeralMessages(destinationUuid, destinationDevice, staleEphemeralMessages);

    return messagesToPublish.name(GET_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry));
  }

  private static boolean isStaleEphemeralMessage(final MessageProtos.Envelope message,
      long earliestAllowableTimestamp) {
    return message.getEphemeral() && message.getClientTimestamp() < earliestAllowableTimestamp;
  }

  private void discardStaleEphemeralMessages(final UUID destinationUuid, final byte destinationDevice,
      Flux<MessageProtos.Envelope> staleEphemeralMessages) {
    staleEphemeralMessages
        .map(e -> UUID.fromString(e.getServerGuid()))
        .buffer(PAGE_SIZE)
        .subscribeOn(messageDeletionScheduler)
        .subscribe(staleEphemeralMessageGuids ->
                remove(destinationUuid, destinationDevice, staleEphemeralMessageGuids)
                    .thenAccept(removedMessages -> staleEphemeralMessagesCounter.increment(removedMessages.size())),
            e -> logger.warn("Could not remove stale ephemeral messages from cache", e));
  }

  @VisibleForTesting
  Flux<MessageProtos.Envelope> getAllMessages(final UUID destinationUuid, final byte destinationDevice) {

    // fetch messages by page
    return getNextMessagePage(destinationUuid, destinationDevice, -1)
        .expand(queueItemsAndLastMessageId -> {
          // expand() is breadth-first, so each page will be published in order
          if (queueItemsAndLastMessageId.first().isEmpty()) {
            return Mono.empty();
          }

          return getNextMessagePage(destinationUuid, destinationDevice, queueItemsAndLastMessageId.second());
        })
        .limitRate(1)
        // we want to ensure we don’t accidentally block the Lettuce/netty i/o executors
        .publishOn(messageDeliveryScheduler)
        .map(Pair::first)
        .concatMap(queueItems -> {

          final List<Mono<MessageProtos.Envelope>> envelopes = new ArrayList<>(queueItems.size() / 2);

          for (int i = 0; i < queueItems.size() - 1; i += 2) {
            try {
              final MessageProtos.Envelope message = MessageProtos.Envelope.parseFrom(queueItems.get(i));

              final Mono<MessageProtos.Envelope> messageMono;
              if (message.hasSharedMrmKey()) {
                final Mono<?> experimentMono = maybeRunMrmViewExperiment(message, destinationUuid, destinationDevice);

                // mrm views phase 1: messageMono for sharedMrmKey is always Mono.just(), because messages always have content
                // To avoid races, wait for the experiment to run, but ignore any errors
                messageMono = experimentMono
                    .onErrorComplete()
                    .then(Mono.just(message.toBuilder().clearSharedMrmKey().build()));
              } else {
                messageMono = Mono.just(message);
              }

              envelopes.add(messageMono);

            } catch (InvalidProtocolBufferException e) {
              logger.warn("Failed to parse envelope", e);
            }
          }

          return Flux.mergeSequential(envelopes);
        });
  }

  /**
   * Runs the fetch and compare logic for the MRM view experiment, if it is enabled.
   *
   * @see DynamicMessagesConfiguration#mrmViewExperimentEnabled()
   */
  private Mono<?> maybeRunMrmViewExperiment(final MessageProtos.Envelope mrmMessage, final UUID destinationUuid,
      final byte destinationDevice) {
    if (dynamicConfigurationManager.getConfiguration().getMessagesConfiguration()
        .mrmViewExperimentEnabled()) {

      final Experiment experiment = new Experiment(MRM_VIEWS_EXPERIMENT_NAME);

      final byte[] key = mrmMessage.getSharedMrmKey().toByteArray();
      final byte[] sharedMrmViewKey = MessagesCache.getSharedMrmViewKey(
          // the message might be addressed to the account's PNI, so use the service ID from the envelope
          ServiceIdentifier.valueOf(mrmMessage.getDestinationServiceId()), destinationDevice);

      final Mono<MessageProtos.Envelope> mrmMessageMono = Mono.from(redisCluster.withBinaryClusterReactive(
          conn -> conn.reactive().hmget(key, "data".getBytes(StandardCharsets.UTF_8), sharedMrmViewKey)
              .collectList()
              .publishOn(messageDeliveryScheduler)
              .handle((mrmDataAndView, sink) -> {
                try {
                  assert mrmDataAndView.size() == 2;

                  final byte[] content = SealedSenderMultiRecipientMessage.messageForRecipient(
                      mrmDataAndView.getFirst().getValue(),
                      mrmDataAndView.getLast().getValue());

                  sink.next(mrmMessage.toBuilder()
                      .clearSharedMrmKey()
                      .setContent(ByteString.copyFrom(content))
                      .build());

                  mrmContentRetrievedCounter.increment();
                } catch (Exception e) {
                  sink.error(e);
                }
              })));

      experiment.compareFutureResult(mrmMessage.toBuilder().clearSharedMrmKey().build(),
          mrmMessageMono.toFuture());

      return mrmMessageMono;
    } else {
      return Mono.empty();
    }
  }

  /**
   * Makes a best-effort attempt at asynchronously updating (and removing when empty) the MRM data structure
   */
  void removeRecipientViewFromMrmData(final List<byte[]> sharedMrmKeys, final UUID accountUuid, final byte deviceId) {

    if (sharedMrmKeys.isEmpty()) {
      return;
    }

    final Timer.Sample sample = Timer.start();
    Flux.fromIterable(sharedMrmKeys)
        .collectMultimap(SlotHash::getSlot)
        .flatMapMany(slotsAndKeys -> Flux.fromIterable(slotsAndKeys.values()))
        .flatMap(
            keys -> removeRecipientViewFromMrmDataScript.execute(keys, new AciServiceIdentifier(accountUuid), deviceId),
            REMOVE_MRM_RECIPIENT_VIEW_CONCURRENCY)
        .doOnNext(sharedMrmDataKeyRemovedCounter::increment)
        .onErrorResume(e -> {
          logger.warn("Error removing recipient view", e);
          return Mono.just(0L);
        })
        .then()
        .doOnTerminate(() -> sample.stop(removeRecipientViewTimer))
        .subscribe();
  }

  private Mono<Pair<List<byte[]>, Long>> getNextMessagePage(final UUID destinationUuid, final byte destinationDevice,
      long messageId) {

    return getItemsScript.execute(destinationUuid, destinationDevice, PAGE_SIZE, messageId)
        .map(queueItems -> {
          logger.trace("Processing page: {}", messageId);

          if (queueItems.isEmpty()) {
            return new Pair<>(Collections.emptyList(), null);
          }

          if (queueItems.size() % 2 != 0) {
            logger.error("\"Get messages\" operation returned a list with a non-even number of elements.");
            return new Pair<>(Collections.emptyList(), null);
          }

          final long lastMessageId = Long.parseLong(
              new String(queueItems.getLast(), StandardCharsets.UTF_8));

          return new Pair<>(queueItems, lastMessageId);
        });
  }

  @VisibleForTesting
  List<MessageProtos.Envelope> getMessagesToPersist(final UUID accountUuid, final byte destinationDevice,
      final int limit) {

    final Timer.Sample sample = Timer.start();

    final List<byte[]> messages = redisCluster.withBinaryCluster(connection ->
        connection.sync().zrange(getMessageQueueKey(accountUuid, destinationDevice), 0, limit));

    return Flux.fromIterable(messages)
        .mapNotNull(message -> {
          try {
            return MessageProtos.Envelope.parseFrom(message);
          } catch (InvalidProtocolBufferException e) {
            logger.warn("Failed to parse envelope", e);
            return null;
          }
        })
        .concatMap(message -> {
          final Mono<MessageProtos.Envelope> messageMono;
          if (message.hasSharedMrmKey()) {
            final Mono<?> experimentMono = maybeRunMrmViewExperiment(message, accountUuid, destinationDevice);

            // mrm views phase 1: messageMono for sharedMrmKey is always Mono.just(), because messages always have content
            // To avoid races, wait for the experiment to run, but ignore any errors
            messageMono = experimentMono
                .onErrorComplete()
                .then(Mono.just(message.toBuilder().clearSharedMrmKey().build()));
          } else {
            messageMono = Mono.just(message);
          }

          return messageMono;
        })
        .collectList()
        .doOnTerminate(() -> sample.stop(getMessagesTimer))
        .block(Duration.ofSeconds(5));
  }

  public CompletableFuture<Void> clear(final UUID destinationUuid) {
    return CompletableFuture.allOf(
        Device.ALL_POSSIBLE_DEVICE_IDS.stream()
            .map(deviceId -> clear(destinationUuid, deviceId))
            .toList()
            .toArray(CompletableFuture[]::new));
  }

  public CompletableFuture<Void> clear(final UUID destinationUuid, final byte deviceId) {
    final Timer.Sample sample = Timer.start();

    return removeQueueScript.execute(destinationUuid, deviceId, Collections.emptyList())
        .publishOn(messageDeletionScheduler)
        .expand(messagesToProcess -> {
          if (messagesToProcess.isEmpty()) {
            return Mono.empty();
          }

          final List<byte[]> mrmKeys = new ArrayList<>(messagesToProcess.size());
          final List<String> processedMessages = new ArrayList<>(messagesToProcess.size());
          for (byte[] serialized : messagesToProcess) {
            try {
              final MessageProtos.Envelope message = MessageProtos.Envelope.parseFrom(serialized);

              processedMessages.add(message.getServerGuid());

              if (message.hasSharedMrmKey()) {
                mrmKeys.add(message.getSharedMrmKey().toByteArray());
              }
            } catch (final InvalidProtocolBufferException e) {
              logger.warn("Failed to parse envelope", e);
            }
          }

          removeRecipientViewFromMrmData(mrmKeys, destinationUuid, deviceId);

          return removeQueueScript.execute(destinationUuid, deviceId, processedMessages);
        })
        .then()
        .toFuture()
        .thenRun(() -> sample.stop(clearQueueTimer));
  }

  int getNextSlotToPersist() {
    return (int) (redisCluster.withCluster(connection -> connection.sync().incr(NEXT_SLOT_TO_PERSIST_KEY))
        % SlotHash.SLOT_COUNT);
  }

  List<String> getQueuesToPersist(final int slot, final Instant maxTime, final int limit) {
    return getQueuesToPersistTimer.record(() -> getQueuesToPersistScript.execute(slot, maxTime, limit));
  }

  void addQueueToPersist(final UUID accountUuid, final byte deviceId) {
    redisCluster.useBinaryCluster(connection -> connection.sync()
        .zadd(getQueueIndexKey(accountUuid, deviceId), ZAddArgs.Builder.nx(), System.currentTimeMillis(),
            getMessageQueueKey(accountUuid, deviceId)));
  }

  void lockQueueForPersistence(final UUID accountUuid, final byte deviceId) {
    redisCluster.useBinaryCluster(
        connection -> connection.sync().setex(getPersistInProgressKey(accountUuid, deviceId), 30, LOCK_VALUE));
  }

  void unlockQueueForPersistence(final UUID accountUuid, final byte deviceId) {
    redisCluster.useBinaryCluster(
        connection -> connection.sync().del(getPersistInProgressKey(accountUuid, deviceId)));
  }

  public void addMessageAvailabilityListener(final UUID destinationUuid, final byte deviceId,
      final MessageAvailabilityListener listener) {
    final String queueName = getQueueName(destinationUuid, deviceId);

    final CompletableFuture<Void> subscribeFuture;
    messageListenersLock.lock();
    try {
      messageListenersByQueueName.put(queueName, listener);
      queueNamesByMessageListener.put(listener, queueName);
      // Submit to the Redis queue while holding the lock, but don’t wait until exiting
      subscribeFuture = subscribeForKeyspaceNotifications(queueName);
    } finally {
      messageListenersLock.unlock();
    }

    subscribeFuture.join();
  }

  public void removeMessageAvailabilityListener(final MessageAvailabilityListener listener) {
    @Nullable final String queueName;
    messageListenersLock.lock();
    try {
      queueName = queueNamesByMessageListener.get(listener);
    } finally {
      messageListenersLock.unlock();
    }

    if (queueName != null) {

      final CompletableFuture<Void> unsubscribeFuture;
      messageListenersLock.lock();
      try {
        queueNamesByMessageListener.remove(listener);
        if (messageListenersByQueueName.remove(queueName, listener)) {
          // Submit to the Redis queue holding the lock, but don’t wait until exiting
          unsubscribeFuture = unsubscribeFromKeyspaceNotifications(queueName);
        } else {
          messageAvailabilityListenerRemovedAfterAddCounter.increment();
          unsubscribeFuture = CompletableFuture.completedFuture(null);
        }
      } finally {
        messageListenersLock.unlock();
      }

      unsubscribeFuture.join();
    }
  }

  private void pruneStaleSubscription(final String channel) {
    unsubscribeFromKeyspaceNotifications(getQueueNameFromKeyspaceChannel(channel))
        .thenRun(prunedStaleSubscriptionCounter::increment);
  }

  private CompletableFuture<Void> subscribeForKeyspaceNotifications(final String queueName) {
    final int slot = SlotHash.getSlot(queueName);

    return pubSubConnection.withPubSubConnection(
            connection -> connection.async()
                .nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(slot))
            .commands()
                .subscribe(getKeyspaceChannels(queueName))).toCompletableFuture()
        .thenRun(Util.NOOP);
  }

  private CompletableFuture<Void> unsubscribeFromKeyspaceNotifications(final String queueName) {
    final int slot = SlotHash.getSlot(queueName);

    return pubSubConnection.withPubSubConnection(
            connection -> connection.async()
                .nodes(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM) && node.hasSlot(slot))
            .commands()
                .unsubscribe(getKeyspaceChannels(queueName)))
        .toCompletableFuture()
        .thenRun(Util.NOOP);
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
          findListener(channel).ifPresentOrElse(listener -> {
            if (!listener.handleNewMessagesAvailable()) {
              removeMessageAvailabilityListener(listener);
            }
          }, () -> pruneStaleSubscription(channel));
        } catch (final Exception e) {
          logger.warn("Unexpected error handling new message", e);
        }
      });
    } else if (channel.startsWith(PERSISTING_KEYSPACE_PREFIX) && "del".equals(message)) {
      queuePersistedNotificationCounter.increment();
      notificationExecutorService.execute(() -> {
        try {
          findListener(channel).ifPresentOrElse(listener -> {
            if (!listener.handleMessagesPersisted()) {
              removeMessageAvailabilityListener(listener);
            }
          }, () -> pruneStaleSubscription(channel));
        } catch (final Exception e) {
          logger.warn("Unexpected error handling messages persisted", e);
        }
      });
    }
  }

  private Optional<MessageAvailabilityListener> findListener(final String keyspaceChannel) {
    final String queueName = getQueueNameFromKeyspaceChannel(keyspaceChannel);

    messageListenersLock.lock();
    try {
      return Optional.ofNullable(messageListenersByQueueName.get(queueName));
    } finally {
      messageListenersLock.unlock();
    }
  }

  @VisibleForTesting
  static String getQueueName(final UUID accountUuid, final byte deviceId) {
    return accountUuid + "::" + deviceId;
  }

  @VisibleForTesting
  static String getQueueNameFromKeyspaceChannel(final String channel) {
    final int startOfHashTag = channel.indexOf('{');
    final int endOfHashTag = channel.lastIndexOf('}');

    return channel.substring(startOfHashTag + 1, endOfHashTag);
  }

  static byte[] getMessageQueueKey(final UUID accountUuid, final byte deviceId) {
    return ("user_queue::{" + accountUuid.toString() + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  static byte[] getMessageQueueMetadataKey(final UUID accountUuid, final byte deviceId) {
    return ("user_queue_metadata::{" + accountUuid.toString() + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  static byte[] getQueueIndexKey(final UUID accountUuid, final byte deviceId) {
    return getQueueIndexKey(SlotHash.getSlot(accountUuid.toString() + "::" + deviceId));
  }

  static byte[] getQueueIndexKey(final int slot) {
    return ("user_queue_index::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}").getBytes(StandardCharsets.UTF_8);
  }

  static byte[] getSharedMrmKey(final UUID mrmGuid) {
    return ("mrm::{" + mrmGuid.toString() + "}").getBytes(StandardCharsets.UTF_8);
  }

  static byte[] getPersistInProgressKey(final UUID accountUuid, final byte deviceId) {
    return ("user_queue_persisting::{" + accountUuid + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
  }

  static byte[] getSharedMrmViewKey(final ServiceId serviceId, final byte deviceId) {
    return getSharedMrmViewKey(serviceId.toServiceIdFixedWidthBinary(), deviceId);
  }

  static byte[] getSharedMrmViewKey(final ServiceIdentifier serviceIdentifier, final byte deviceId) {
    return getSharedMrmViewKey(serviceIdentifier.toFixedWidthByteArray(), deviceId);
  }

  private static byte[] getSharedMrmViewKey(final byte[] fixedWithServiceId, final byte deviceId) {
    assert fixedWithServiceId.length == 17;

    final ByteBuffer keyBb = ByteBuffer.allocate(18);
    keyBb.put(fixedWithServiceId);
    keyBb.put(deviceId);
    assert !keyBb.hasRemaining();
    return keyBb.array();
  }

  static UUID getAccountUuidFromQueueName(final String queueName) {
    final int startOfHashTag = queueName.indexOf('{');

    return UUID.fromString(queueName.substring(startOfHashTag + 1, queueName.indexOf("::", startOfHashTag)));
  }

  static byte getDeviceIdFromQueueName(final String queueName) {
    return Byte.parseByte(queueName.substring(queueName.lastIndexOf("::") + 2, queueName.lastIndexOf('}')));
  }
}
