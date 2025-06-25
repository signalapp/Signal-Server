/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.cluster.SlotHash;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.ServiceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Manages short-term storage of messages in Redis. Messages are frequently delivered to their destination and deleted
 * shortly after they reach the server, and this cache acts as a low-latency holding area for new messages, reducing
 * load on higher-latency, longer-term storage systems.
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
public class MessagesCache {

  private final FaultTolerantRedisClusterClient redisCluster;
  private final Clock clock;

  private final Scheduler messageDeliveryScheduler;
  private final ExecutorService messageDeletionExecutorService;
  // messageDeletionExecutorService wrapped into a reactor Scheduler
  private final Scheduler messageDeletionScheduler;

  private final MessagesCacheInsertScript insertScript;
  private final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript;
  private final MessagesCacheRemoveByGuidScript removeByGuidScript;
  private final MessagesCacheGetItemsScript getItemsScript;
  private final MessagesCacheRemoveQueueScript removeQueueScript;
  private final MessagesCacheGetQueuesToPersistScript getQueuesToPersistScript;
  private final MessagesCacheRemoveRecipientViewFromMrmDataScript removeRecipientViewFromMrmDataScript;
  private final MessagesCacheUnlockQueueScript unlockQueueScript;

  private final Timer insertTimer = Metrics.timer(name(MessagesCache.class, "insert"));
  private final Timer insertSharedMrmPayloadTimer = Metrics.timer(name(MessagesCache.class, "insertSharedMrmPayload"));
  private final Timer getMessagesTimer = Metrics.timer(name(MessagesCache.class, "get"));
  private final Timer getQueuesToPersistTimer = Metrics.timer(name(MessagesCache.class, "getQueuesToPersist"));
  private final Timer removeByGuidTimer = Metrics.timer(name(MessagesCache.class, "removeByGuid"));
  private final Timer removeRecipientViewTimer = Metrics.timer(name(MessagesCache.class, "removeRecipientView"));
  private final Timer clearQueueTimer = Metrics.timer(name(MessagesCache.class, "clear"));
  private final Counter removeMessageCounter = Metrics.counter(name(MessagesCache.class, "remove"));
  private final Counter staleEphemeralMessagesCounter = Metrics.counter(
      name(MessagesCache.class, "staleEphemeralMessages"));
  private final Counter staleMrmMessagesCounter = Metrics.counter(name(MessagesCache.class, "staleMrmMessages"));
  private final Counter mrmContentRetrievedCounter = Metrics.counter(name(MessagesCache.class, "mrmViewRetrieved"));
  private final String MRM_RETRIEVAL_ERROR_COUNTER_NAME = name(MessagesCache.class, "mrmRetrievalError");
  private final String EPHEMERAL_TAG_NAME = "ephemeral";
  private final String MISSING_MRM_DATA_TAG_NAME = "missingMrmData";
  private final Counter skippedStaleEphemeralMrmCounter = Metrics.counter(
      name(MessagesCache.class, "skippedStaleEphemeralMrm"));
  private final Counter sharedMrmDataKeyRemovedCounter = Metrics.counter(
      name(MessagesCache.class, "sharedMrmKeyRemoved"));

  static final String NEXT_SLOT_TO_PERSIST_KEY = "user_queue_persist_slot";
  private static final byte[] LOCK_VALUE = "1".getBytes(StandardCharsets.UTF_8);

  @VisibleForTesting
  static final ByteString STALE_MRM_KEY = ByteString.copyFromUtf8("stale");

  @VisibleForTesting
  static final Duration MAX_EPHEMERAL_MESSAGE_DELAY = Duration.ofSeconds(10);

  private static final String GET_FLUX_NAME = MetricsUtil.name(MessagesCache.class, "get");
  private static final int PAGE_SIZE = 100;

  private static final int REMOVE_MRM_RECIPIENT_VIEW_CONCURRENCY = 8;

  private static final Logger logger = LoggerFactory.getLogger(MessagesCache.class);

  public MessagesCache(final FaultTolerantRedisClusterClient redisCluster,
      final Scheduler messageDeliveryScheduler,
      final ExecutorService messageDeletionExecutorService,
      final Clock clock)
      throws IOException {

    this(
        redisCluster,
        messageDeliveryScheduler,
        messageDeletionExecutorService,
        clock,
        new MessagesCacheInsertScript(redisCluster),
        new MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(redisCluster),
        new MessagesCacheGetItemsScript(redisCluster),
        new MessagesCacheRemoveByGuidScript(redisCluster),
        new MessagesCacheRemoveQueueScript(redisCluster),
        new MessagesCacheGetQueuesToPersistScript(redisCluster),
        new MessagesCacheRemoveRecipientViewFromMrmDataScript(redisCluster),
        new MessagesCacheUnlockQueueScript(redisCluster)
    );
  }

  @VisibleForTesting
  MessagesCache(final FaultTolerantRedisClusterClient redisCluster,
                final Scheduler messageDeliveryScheduler,
                final ExecutorService messageDeletionExecutorService, final Clock clock,
                final MessagesCacheInsertScript insertScript,
                final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript,
                final MessagesCacheGetItemsScript getItemsScript, final MessagesCacheRemoveByGuidScript removeByGuidScript,
                final MessagesCacheRemoveQueueScript removeQueueScript,
                final MessagesCacheGetQueuesToPersistScript getQueuesToPersistScript,
                final MessagesCacheRemoveRecipientViewFromMrmDataScript removeRecipientViewFromMrmDataScript,
                final MessagesCacheUnlockQueueScript unlockQueueScript) throws IOException {

    this.redisCluster = redisCluster;
    this.clock = clock;

    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.messageDeletionExecutorService = messageDeletionExecutorService;
    this.messageDeletionScheduler = Schedulers.fromExecutorService(messageDeletionExecutorService, "messageDeletion");

    this.insertScript = insertScript;
    this.insertMrmScript = insertMrmScript;
    this.removeByGuidScript = removeByGuidScript;
    this.getItemsScript = getItemsScript;
    this.removeQueueScript = removeQueueScript;
    this.getQueuesToPersistScript = getQueuesToPersistScript;
    this.removeRecipientViewFromMrmDataScript = removeRecipientViewFromMrmDataScript;
    this.unlockQueueScript = unlockQueueScript;
  }

  public CompletableFuture<Boolean> insert(final UUID messageGuid,
      final UUID destinationAccountIdentifier,
      final byte destinationDeviceId,
      final MessageProtos.Envelope message) {

    final MessageProtos.Envelope messageWithGuid = message.toBuilder().setServerGuid(messageGuid.toString()).build();
    final Timer.Sample sample = Timer.start();

    return insertScript.executeAsync(destinationAccountIdentifier, destinationDeviceId, messageWithGuid)
        .whenComplete((ignored, throwable) -> sample.stop(insertTimer));
  }

  public CompletableFuture<byte[]> insertSharedMultiRecipientMessagePayload(
      final SealedSenderMultiRecipientMessage sealedSenderMultiRecipientMessage) {

    final Timer.Sample sample = Timer.start();

    final byte[] sharedMrmKey = getSharedMrmKey(UUID.randomUUID());

    return insertMrmScript.executeAsync(sharedMrmKey, sealedSenderMultiRecipientMessage)
        .thenApply(ignored -> sharedMrmKey)
        .whenComplete((ignored, throwable) -> sample.stop(insertSharedMrmPayloadTimer));
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
          final Map<ServiceIdentifier, List<byte[]>> serviceIdentifierToMrmKeys = new HashMap<>();

          for (final byte[] bytes : serialized) {
            try {
              final MessageProtos.Envelope envelope = parseEnvelope(bytes);
              removedMessages.add(RemovedMessage.fromEnvelope(envelope));
              if (envelope.hasSharedMrmKey()) {
                serviceIdentifierToMrmKeys.computeIfAbsent(
                        ServiceIdentifier.valueOf(envelope.getDestinationServiceId()), ignored -> new ArrayList<>())
                    .add(envelope.getSharedMrmKey().toByteArray());
              }
            } catch (final InvalidProtocolBufferException e) {
              logger.warn("Failed to parse envelope", e);
            }
          }

          serviceIdentifierToMrmKeys.forEach(
              (serviceId, keysToUpdate) -> removeRecipientViewFromMrmData(keysToUpdate, serviceId, destinationDevice));

          return removedMessages;
        }, messageDeletionExecutorService).whenComplete((removedMessages, throwable) -> {
          if (removedMessages != null) {
            removeMessageCounter.increment(removedMessages.size());
          }

          sample.stop(removeByGuidTimer);
        });

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

    final Flux<MessageProtos.Envelope> allMessages = getAllMessages(destinationUuid, destinationDevice,
        earliestAllowableEphemeralTimestamp, PAGE_SIZE)
        .publish()
        // We expect exactly three subscribers to this base flux:
        // 1. the websocket that delivers messages to clients
        // 2. an internal processes to discard stale ephemeral messages
        // 3. an internal process to discard stale MRM messages
        // The discard subscribers will subscribe immediately, but we don’t want to do any work if the
        // websocket never subscribes.
        .autoConnect(3);

    final Flux<MessageProtos.Envelope> messagesToPublish = allMessages
        .filter(Predicate.not(envelope ->
            isStaleEphemeralMessage(envelope, earliestAllowableEphemeralTimestamp) || isStaleMrmMessage(envelope)));

    final Flux<MessageProtos.Envelope> staleEphemeralMessages = allMessages
        .filter(envelope -> isStaleEphemeralMessage(envelope, earliestAllowableEphemeralTimestamp));
    discardStaleMessages(destinationUuid, destinationDevice, staleEphemeralMessages, staleEphemeralMessagesCounter, "ephemeral");

    final Flux<MessageProtos.Envelope> staleMrmMessages = allMessages.filter(MessagesCache::isStaleMrmMessage)
        // clearing the sharedMrmKey prevents unnecessary calls to update the shared MRM data
        .map(envelope -> envelope.toBuilder().clearSharedMrmKey().build());
    discardStaleMessages(destinationUuid, destinationDevice, staleMrmMessages, staleMrmMessagesCounter, "mrm");

    return messagesToPublish.name(GET_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry));
  }

  public Mono<Long> getEarliestUndeliveredTimestamp(final UUID destinationUuid, final byte destinationDevice) {
    return getAllMessages(destinationUuid, destinationDevice, -1, 1)
        .next()
        .map(MessageProtos.Envelope::getServerTimestamp);
  }

  private static boolean isStaleEphemeralMessage(final MessageProtos.Envelope message,
      long earliestAllowableTimestamp) {
    return message.getEphemeral() && message.getClientTimestamp() < earliestAllowableTimestamp;
  }

  /**
   * Checks whether the given message is a stale MRM message
   *
   * @see #getMessageWithSharedMrmData(MessageProtos.Envelope, byte)
   */
  private static boolean isStaleMrmMessage(final MessageProtos.Envelope message) {
    return message.hasSharedMrmKey() && STALE_MRM_KEY.equals(message.getSharedMrmKey());
  }

  private void discardStaleMessages(final UUID destinationUuid, final byte destinationDevice,
      Flux<MessageProtos.Envelope> staleMessages, final Counter counter, final String context) {
    staleMessages
        .map(e -> UUID.fromString(e.getServerGuid()))
        .buffer(PAGE_SIZE)
        .subscribeOn(messageDeletionScheduler)
        .subscribe(messageGuids ->
                remove(destinationUuid, destinationDevice, messageGuids)
                    .thenAccept(removedMessages -> counter.increment(removedMessages.size())),
            e -> logger.warn("Could not remove stale {} messages from cache", context, e));
  }

  @VisibleForTesting
  Flux<MessageProtos.Envelope> getAllMessages(final UUID destinationUuid, final byte destinationDevice,
      final long earliestAllowableEphemeralTimestamp, final int pageSize) {

    // fetch messages by page
    return getNextMessagePage(destinationUuid, destinationDevice, -1, pageSize)
        .expand(queueItemsAndLastMessageId -> {
          // expand() is breadth-first, so each page will be published in order
          if (queueItemsAndLastMessageId.first().isEmpty()) {
            return Mono.empty();
          }

          return getNextMessagePage(destinationUuid, destinationDevice, queueItemsAndLastMessageId.second(), pageSize);
        })
        .limitRate(1)
        // we want to ensure we don’t accidentally block the Lettuce/netty i/o executors
        .publishOn(messageDeliveryScheduler)
        .map(Pair::first)
        .concatMap(queueItems -> {

          final List<Mono<MessageProtos.Envelope>> envelopes = new ArrayList<>(queueItems.size() / 2);

          for (int i = 0; i < queueItems.size() - 1; i += 2) {
            try {
              final MessageProtos.Envelope message = parseEnvelope(queueItems.get(i));

              final Mono<MessageProtos.Envelope> messageMono;
              if (message.hasSharedMrmKey()) {

                if (isStaleEphemeralMessage(message, earliestAllowableEphemeralTimestamp)) {
                  // skip fetching content for message that will be discarded
                  messageMono = Mono.just(message.toBuilder().clearSharedMrmKey().build());
                  skippedStaleEphemeralMrmCounter.increment();
                } else {
                  messageMono = getMessageWithSharedMrmData(message, destinationDevice);
                }

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
   * Returns the given message with its shared MRM data. There are three possible cases:
   * <ol>
   *   <li>The reconstructed message for delivery with {@code content} set and {@code sharedMrmKey} cleared</li>
   *   <li>The input with {@code sharedMrmKey} set to a static value, indicating that the shared MRM data is no longer available, and the message should be
   *       discarded from the queue</li>
   *   <li>An empty {@code Mono}, if an unexpected error occurred</li>
   * </ol>
   */
  private Mono<MessageProtos.Envelope> getMessageWithSharedMrmData(final MessageProtos.Envelope mrmMessage,
      final byte destinationDevice) {

    assert mrmMessage.hasSharedMrmKey();

    final byte[] key = mrmMessage.getSharedMrmKey().toByteArray();
    final byte[] sharedMrmViewKey = MessagesCache.getSharedMrmViewKey(
        // the message might be addressed to the account's PNI, so use the service ID from the envelope
        ServiceIdentifier.valueOf(mrmMessage.getDestinationServiceId()), destinationDevice);

    return Mono.from(redisCluster.withBinaryClusterReactive(
            conn -> conn.reactive().hmget(key, "data".getBytes(StandardCharsets.UTF_8), sharedMrmViewKey)
                .collectList()
                .publishOn(messageDeliveryScheduler)))
        .<MessageProtos.Envelope>handle((mrmDataAndView, sink) -> {
          try {
            assert mrmDataAndView.size() == 2;

            if (mrmDataAndView.getFirst().isEmpty()) {
              // shared data is missing
              //noinspection ReactiveStreamsThrowInOperator
              throw new MrmDataMissingException(MrmDataMissingException.Type.SHARED);
            }

            if (mrmDataAndView.getLast().isEmpty()) {
              // recipient's view is missing
              //noinspection ReactiveStreamsThrowInOperator
              throw new MrmDataMissingException(MrmDataMissingException.Type.RECIPIENT_VIEW);
            }

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
        })
        .onErrorResume(throwable -> {

          final List<Tag> tags = new ArrayList<>();
          tags.add(Tag.of(EPHEMERAL_TAG_NAME, String.valueOf(mrmMessage.getEphemeral())));

          final Mono<MessageProtos.Envelope> result;
          if (throwable instanceof MrmDataMissingException mdme) {
            tags.add(Tag.of(MISSING_MRM_DATA_TAG_NAME, mdme.getType().name()));
            // MRM data may be missing if either of the two non-transactional writes (delete from queue, update shared
            // MRM data) fails after it has been delivered. We return it so that it may be discarded from the queue.
            result = Mono.just(mrmMessage.toBuilder().setSharedMrmKey(STALE_MRM_KEY).build());
          } else {
            logger.warn("Failed to retrieve shared mrm data", throwable);
            // For unexpected errors, return empty. The message will remain in the queue and be retried in the future.
            result = Mono.empty();
          }

          Metrics.counter(MRM_RETRIEVAL_ERROR_COUNTER_NAME, tags).increment();

          return result;
        })
        .share();
  }

  /**
   * Makes a best-effort attempt at asynchronously updating (and removing when empty) the MRM data structure
   */
  void removeRecipientViewFromMrmData(final List<byte[]> sharedMrmKeys, final ServiceIdentifier serviceIdentifier,
      final byte deviceId) {

    if (sharedMrmKeys.isEmpty()) {
      return;
    }

    final Timer.Sample sample = Timer.start();
    Flux.fromIterable(sharedMrmKeys)
        .collectMultimap(SlotHash::getSlot)
        .flatMapMany(slotsAndKeys -> Flux.fromIterable(slotsAndKeys.values()))
        .flatMap(
            keys -> removeRecipientViewFromMrmDataScript.execute(keys, serviceIdentifier, deviceId),
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
      long messageId, int pageSize) {

    return getItemsScript.execute(destinationUuid, destinationDevice, pageSize, messageId)
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

  /**
   * Estimate the size of the cached queue if it were to be persisted
   * @param accountUuid The account identifier
   * @param destinationDevice The destination device id
   * @return A future that completes with the approximate size of stored messages that need to be persisted
   */
  CompletableFuture<Long> estimatePersistedQueueSizeBytes(final UUID accountUuid, final byte destinationDevice) {
    final Function<Optional<Long>, Mono<List<ScoredValue<byte[]>>>> getNextPage = (Optional<Long> start) ->
        Mono.fromCompletionStage(() -> redisCluster.withBinaryCluster(connection ->
            connection.async().zrangebyscoreWithScores(
                getMessageQueueKey(accountUuid, destinationDevice),
                Range.from(
                    start.map(Range.Boundary::excluding).orElse(Range.Boundary.unbounded()),
                    Range.Boundary.unbounded()),
                Limit.from(PAGE_SIZE))));
    final Flux<byte[]> allSerializedMessages = getNextPage.apply(Optional.empty())
        .expand(scoredValues -> {
          if (scoredValues.isEmpty()) {
            return Mono.empty();
          }
          long lastTimestamp = (long) scoredValues.getLast().getScore();
          return getNextPage.apply(Optional.of(lastTimestamp));
        })
        .concatMap(scoredValues -> Flux.fromStream(scoredValues.stream().map(ScoredValue::getValue)));

    return parseAndFetchMrms(allSerializedMessages, destinationDevice)
        .filter(Predicate.not(envelope -> envelope.getEphemeral() || isStaleMrmMessage(envelope)))
        .reduce(0L, (acc, envelope) -> acc + envelope.getSerializedSize())
        .toFuture();
  }

  List<MessageProtos.Envelope> getMessagesToPersist(final UUID accountUuid, final byte destinationDevice,
      final int limit) {

    final Timer.Sample sample = Timer.start();

    final List<byte[]> messages = redisCluster.withBinaryCluster(connection ->
        connection.sync().zrange(getMessageQueueKey(accountUuid, destinationDevice), 0, limit));

    final Flux<MessageProtos.Envelope> allMessages = parseAndFetchMrms(Flux.fromIterable(messages), destinationDevice);

    final Flux<MessageProtos.Envelope> messagesToPersist = allMessages
        .filter(Predicate.not(envelope ->
            envelope.getEphemeral() || isStaleMrmMessage(envelope)));

    final Flux<MessageProtos.Envelope> ephemeralMessages = allMessages
        .filter(MessageProtos.Envelope::getEphemeral);
    discardStaleMessages(accountUuid, destinationDevice, ephemeralMessages, staleEphemeralMessagesCounter, "ephemeral");

    final Flux<MessageProtos.Envelope> staleMrmMessages = allMessages.filter(MessagesCache::isStaleMrmMessage)
        // clearing the sharedMrmKey prevents unnecessary calls to update the shared MRM data
        .map(envelope -> envelope.toBuilder().clearSharedMrmKey().build());
    discardStaleMessages(accountUuid, destinationDevice, staleMrmMessages, staleMrmMessagesCounter, "mrm");

    return messagesToPersist
        .collectList()
        .doOnTerminate(() -> sample.stop(getMessagesTimer))
        .block(Duration.ofSeconds(5));
  }

  private Flux<MessageProtos.Envelope> parseAndFetchMrms(final Flux<byte[]> serializedMessages, final byte destinationDevice) {
    return serializedMessages
        .mapNotNull(message -> {
          try {
            return parseEnvelope(message);
          } catch (InvalidProtocolBufferException e) {
            logger.warn("Failed to parse envelope", e);
            return null;
          }
        })
        .concatMap(message -> {
          final Mono<MessageProtos.Envelope> messageMono;
          if (message.hasSharedMrmKey()) {
            messageMono = getMessageWithSharedMrmData(message, destinationDevice);
          } else {
            messageMono = Mono.just(message);
          }

          return messageMono;
        });
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

          final Map<ServiceIdentifier, List<byte[]>> serviceIdentifierToMrmKeys = new HashMap<>();
          final List<String> processedMessages = new ArrayList<>(messagesToProcess.size());
          for (byte[] serialized : messagesToProcess) {
            try {
              final MessageProtos.Envelope message = parseEnvelope(serialized);

              processedMessages.add(message.getServerGuid());

              if (message.hasSharedMrmKey()) {
                serviceIdentifierToMrmKeys.computeIfAbsent(ServiceIdentifier.valueOf(message.getDestinationServiceId()),
                        ignored -> new ArrayList<>())
                    .add(message.getSharedMrmKey().toByteArray());
              }
            } catch (final InvalidProtocolBufferException e) {
              logger.warn("Failed to parse envelope", e);
            }
          }

          serviceIdentifierToMrmKeys.forEach((serviceId, keysToUpdate) ->
              removeRecipientViewFromMrmData(keysToUpdate, serviceId, deviceId));

          return removeQueueScript.execute(destinationUuid, deviceId, processedMessages);
        })
        .then()
        .toFuture()
        .thenRun(() -> sample.stop(clearQueueTimer));
  }

  public String shardForSlot(int slot) {
    try {
      return redisCluster.withBinaryCluster(
          connection -> connection.getPartitions().getPartitionBySlot(slot).getUri().getHost());
    } catch (Throwable ignored) {
      return "unknown";
    }
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
    unlockQueueScript.execute(accountUuid, deviceId);
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

  private static MessageProtos.Envelope parseEnvelope(final byte[] envelopeBytes)
      throws InvalidProtocolBufferException {

    return EnvelopeUtil.expand(MessageProtos.Envelope.parseFrom(envelopeBytes));
  }
}
