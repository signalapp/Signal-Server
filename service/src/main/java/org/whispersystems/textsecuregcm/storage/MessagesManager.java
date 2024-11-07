/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.Pair;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MessagesManager {

  private static final int RESULT_SET_CHUNK_SIZE = 100;
  final String GET_MESSAGES_FOR_DEVICE_FLUX_NAME = name(MessagesManager.class, "getMessagesForDevice");

  private static final Logger logger = LoggerFactory.getLogger(MessagesManager.class);

  private static final Counter PERSIST_MESSAGE_COUNTER = Metrics.counter(
      name(MessagesManager.class, "persistMessage"));

  private static final String MAY_HAVE_MESSAGES_COUNTER_NAME =
      MetricsUtil.name(MessagesManager.class, "mayHaveMessages");

  private final MessagesDynamoDb messagesDynamoDb;
  private final MessagesCache messagesCache;
  private final ReportMessageManager reportMessageManager;
  private final ExecutorService messageDeletionExecutor;

  public MessagesManager(
      final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final ReportMessageManager reportMessageManager,
      final ExecutorService messageDeletionExecutor) {
    this.messagesDynamoDb = messagesDynamoDb;
    this.messagesCache = messagesCache;
    this.reportMessageManager = reportMessageManager;
    this.messageDeletionExecutor = messageDeletionExecutor;
  }

  /**
   * Inserts a message into a target device's message queue and notifies registered listeners that a new message is
   * available.
   *
   * @param destinationUuid the account identifier for the destination queue
   * @param destinationDeviceId the device ID for the destination queue
   * @param message the message to insert into the queue
   *
   * @return {@code true} if the destination device is "present" (i.e. has an active event listener) or {@code false}
   * otherwise
   *
   * @see org.whispersystems.textsecuregcm.push.WebSocketConnectionEventManager
   */
  public boolean insert(final UUID destinationUuid, final byte destinationDeviceId, final Envelope message) {
    final UUID messageGuid = UUID.randomUUID();

    final boolean destinationPresent = messagesCache.insert(messageGuid, destinationUuid, destinationDeviceId, message);

    if (message.hasSourceServiceId() && !destinationUuid.toString().equals(message.getSourceServiceId())) {
      reportMessageManager.store(message.getSourceServiceId(), messageGuid);
    }

    return destinationPresent;
  }

  public CompletableFuture<Boolean> mayHavePersistedMessages(final UUID destinationUuid, final Device destinationDevice) {
    return messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice);
  }

  public CompletableFuture<Boolean> mayHaveMessages(final UUID destinationUuid, final Device destinationDevice) {
    return messagesCache.hasMessagesAsync(destinationUuid, destinationDevice.getId())
        .thenCombine(messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice),
            (mayHaveCachedMessages, mayHavePersistedMessages) -> {
              final String outcome;

              if (mayHaveCachedMessages && mayHavePersistedMessages) {
                outcome = "both";
              } else if (mayHaveCachedMessages) {
                outcome = "cached";
              } else if (mayHavePersistedMessages) {
                outcome = "persisted";
              } else {
                outcome = "none";
              }

              Metrics.counter(MAY_HAVE_MESSAGES_COUNTER_NAME, "outcome", outcome).increment();

              return mayHaveCachedMessages || mayHavePersistedMessages;
            });
  }

  public CompletableFuture<Boolean> mayHaveUrgentPersistedMessages(final UUID destinationUuid, final Device destinationDevice) {
    return messagesDynamoDb.mayHaveUrgentMessages(destinationUuid, destinationDevice);
  }

  public Mono<Pair<List<Envelope>, Boolean>> getMessagesForDevice(UUID destinationUuid, Device destinationDevice,
      boolean cachedMessagesOnly) {

    return Flux.from(
            getMessagesForDevice(destinationUuid, destinationDevice, RESULT_SET_CHUNK_SIZE, cachedMessagesOnly))
        .take(RESULT_SET_CHUNK_SIZE)
        .collectList()
        .map(envelopes -> new Pair<>(envelopes, envelopes.size() >= RESULT_SET_CHUNK_SIZE));
  }

  public Publisher<Envelope> getMessagesForDeviceReactive(UUID destinationUuid, Device destinationDevice,
      final boolean cachedMessagesOnly) {

    return getMessagesForDevice(destinationUuid, destinationDevice, null, cachedMessagesOnly);
  }

  private Publisher<Envelope> getMessagesForDevice(UUID destinationUuid, Device destinationDevice,
      @Nullable Integer limit, final boolean cachedMessagesOnly) {

    final Publisher<Envelope> dynamoPublisher =
        cachedMessagesOnly ? Flux.empty() : messagesDynamoDb.load(destinationUuid, destinationDevice, limit);
    final Publisher<Envelope> cachePublisher = messagesCache.get(destinationUuid, destinationDevice.getId());

    return Flux.concat(dynamoPublisher, cachePublisher)
        .name(GET_MESSAGES_FOR_DEVICE_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry));
  }

  public CompletableFuture<Void> clear(UUID destinationUuid) {
    return messagesCache.clear(destinationUuid);
  }

  public CompletableFuture<Void> clear(UUID destinationUuid, byte deviceId) {
    return messagesCache.clear(destinationUuid, deviceId);
  }

  public CompletableFuture<Optional<RemovedMessage>> delete(UUID destinationUuid, Device destinationDevice, UUID guid,
      @Nullable Long serverTimestamp) {
    return messagesCache.remove(destinationUuid, destinationDevice.getId(), guid)
        .thenComposeAsync(removed -> {

          if (removed.isPresent()) {
            return CompletableFuture.completedFuture(removed);
          }

          final CompletableFuture<Optional<MessageProtos.Envelope>> maybeDeletedEnvelope;
          if (serverTimestamp == null) {
            maybeDeletedEnvelope = messagesDynamoDb.deleteMessageByDestinationAndGuid(destinationUuid,
                destinationDevice, guid);
          } else {
            maybeDeletedEnvelope = messagesDynamoDb.deleteMessage(destinationUuid, destinationDevice, guid,
                serverTimestamp);
          }

          return maybeDeletedEnvelope.thenApply(maybeEnvelope -> maybeEnvelope.map(RemovedMessage::fromEnvelope));
        }, messageDeletionExecutor);
  }

  /**
   * @return the number of messages successfully removed from the cache.
   */
  public int persistMessages(
      final UUID destinationUuid,
      final Device destinationDevice,
      final List<Envelope> messages) {

    final List<Envelope> nonEphemeralMessages = messages.stream()
        .filter(envelope -> !envelope.getEphemeral())
        .collect(Collectors.toList());

    messagesDynamoDb.store(nonEphemeralMessages, destinationUuid, destinationDevice);

    final List<UUID> messageGuids = messages.stream().map(message -> UUID.fromString(message.getServerGuid()))
        .collect(Collectors.toList());
    int messagesRemovedFromCache = 0;
    try {
      messagesRemovedFromCache = messagesCache.remove(destinationUuid, destinationDevice.getId(), messageGuids)
          .get(30, TimeUnit.SECONDS).size();
      PERSIST_MESSAGE_COUNTER.increment(nonEphemeralMessages.size());

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.warn("Failed to remove messages from cache", e);
    }
    return messagesRemovedFromCache;
  }

  public CompletableFuture<Optional<Instant>> getEarliestUndeliveredTimestampForDevice(UUID destinationUuid, Device destinationDevice) {
    // If there's any message in the persisted layer, return the oldest
    return Mono.from(messagesDynamoDb.load(destinationUuid, destinationDevice, 1)).map(Envelope::getServerTimestamp)
        // If not, return the oldest message in the cache
        .switchIfEmpty(messagesCache.getEarliestUndeliveredTimestamp(destinationUuid, destinationDevice.getId()))
        .map(epochMilli -> Optional.of(Instant.ofEpochMilli(epochMilli)))
        .switchIfEmpty(Mono.just(Optional.empty()))
        .toFuture();
  }

  /**
   * Inserts the shared multi-recipient message payload to storage.
   *
   * @return a key where the shared data is stored
   * @see MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript
   */
  public byte[] insertSharedMultiRecipientMessagePayload(
      final SealedSenderMultiRecipientMessage sealedSenderMultiRecipientMessage) {
    return messagesCache.insertSharedMultiRecipientMessagePayload(sealedSenderMultiRecipientMessage);
  }
}
