/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MessagesManager {

  private static final int RESULT_SET_CHUNK_SIZE = 100;
  final String GET_MESSAGES_FOR_DEVICE_FLUX_NAME = MetricsUtil.name(MessagesManager.class, "getMessagesForDevice");

  private static final Logger logger = LoggerFactory.getLogger(MessagesManager.class);

  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter cacheHitByGuidMeter = metricRegistry.meter(name(MessagesManager.class, "cacheHitByGuid"));
  private static final Meter cacheMissByGuidMeter = metricRegistry.meter(
      name(MessagesManager.class, "cacheMissByGuid"));
  private static final Meter persistMessageMeter = metricRegistry.meter(name(MessagesManager.class, "persistMessage"));

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

  public void insert(UUID destinationUuid, long destinationDevice, Envelope message) {
    final UUID messageGuid = UUID.randomUUID();

    messagesCache.insert(messageGuid, destinationUuid, destinationDevice, message);

    if (message.hasSourceUuid() && !destinationUuid.toString().equals(message.getSourceUuid())) {
      reportMessageManager.store(message.getSourceUuid(), messageGuid);
    }
  }

  public boolean hasCachedMessages(final UUID destinationUuid, final long destinationDevice) {
    return messagesCache.hasMessages(destinationUuid, destinationDevice);
  }

  public Mono<Pair<List<Envelope>, Boolean>> getMessagesForDevice(UUID destinationUuid, long destinationDevice,
      boolean cachedMessagesOnly) {

    return Flux.from(
            getMessagesForDevice(destinationUuid, destinationDevice, RESULT_SET_CHUNK_SIZE, cachedMessagesOnly))
        .take(RESULT_SET_CHUNK_SIZE, true)
        .collectList()
        .map(envelopes -> new Pair<>(envelopes, envelopes.size() >= RESULT_SET_CHUNK_SIZE));
  }

  public Publisher<Envelope> getMessagesForDeviceReactive(UUID destinationUuid, long destinationDevice,
      final boolean cachedMessagesOnly) {

    return getMessagesForDevice(destinationUuid, destinationDevice, null, cachedMessagesOnly);
  }

  private Publisher<Envelope> getMessagesForDevice(UUID destinationUuid, long destinationDevice,
      @Nullable Integer limit, final boolean cachedMessagesOnly) {

    final Publisher<Envelope> dynamoPublisher =
        cachedMessagesOnly ? Flux.empty() : messagesDynamoDb.load(destinationUuid, destinationDevice, limit);
    final Publisher<Envelope> cachePublisher = messagesCache.get(destinationUuid, destinationDevice);

    return Flux.concat(dynamoPublisher, cachePublisher)
        .name(GET_MESSAGES_FOR_DEVICE_FLUX_NAME)
        .metrics();
  }

  public void clear(UUID destinationUuid) {
    messagesCache.clear(destinationUuid);
    messagesDynamoDb.deleteAllMessagesForAccount(destinationUuid);
  }

  public void clear(UUID destinationUuid, long deviceId) {
    messagesCache.clear(destinationUuid, deviceId);
    messagesDynamoDb.deleteAllMessagesForDevice(destinationUuid, deviceId);
  }

  public CompletableFuture<Optional<Envelope>> delete(UUID destinationUuid, long destinationDeviceId, UUID guid,
      @Nullable Long serverTimestamp) {
    return messagesCache.remove(destinationUuid, destinationDeviceId, guid)
        .thenComposeAsync(removed -> {

          if (removed.isPresent()) {
            cacheHitByGuidMeter.mark();
            return CompletableFuture.completedFuture(removed);
          }

          cacheMissByGuidMeter.mark();

          if (serverTimestamp == null) {
            return messagesDynamoDb.deleteMessageByDestinationAndGuid(destinationUuid, guid);
          } else {
            return messagesDynamoDb.deleteMessage(destinationUuid, destinationDeviceId, guid, serverTimestamp);
          }

        }, messageDeletionExecutor);
  }

  /**
   * @return the number of messages successfully removed from the cache.
   */
  public int persistMessages(
      final UUID destinationUuid,
      final long destinationDeviceId,
      final List<Envelope> messages) {

    final List<Envelope> nonEphemeralMessages = messages.stream()
        .filter(envelope -> !envelope.getEphemeral())
        .collect(Collectors.toList());

    messagesDynamoDb.store(nonEphemeralMessages, destinationUuid, destinationDeviceId);

    final List<UUID> messageGuids = messages.stream().map(message -> UUID.fromString(message.getServerGuid()))
        .collect(Collectors.toList());
    int messagesRemovedFromCache = 0;
    try {
      messagesRemovedFromCache = messagesCache.remove(destinationUuid, destinationDeviceId, messageGuids)
          .get(30, TimeUnit.SECONDS).size();
      persistMessageMeter.mark(nonEphemeralMessages.size());

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.warn("Failed to remove messages from cache", e);
    }
    return messagesRemovedFromCache;
  }

  public void addMessageAvailabilityListener(
      final UUID destinationUuid,
      final long destinationDeviceId,
      final MessageAvailabilityListener listener) {
    messagesCache.addMessageAvailabilityListener(destinationUuid, destinationDeviceId, listener);
  }

  public void removeMessageAvailabilityListener(final MessageAvailabilityListener listener) {
    messagesCache.removeMessageAvailabilityListener(listener);
  }

}
