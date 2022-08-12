/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;

public class MessagesManager {

  private static final int RESULT_SET_CHUNK_SIZE = 100;

  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter cacheHitByGuidMeter = metricRegistry.meter(name(MessagesManager.class, "cacheHitByGuid"));
  private static final Meter cacheMissByGuidMeter = metricRegistry.meter(
      name(MessagesManager.class, "cacheMissByGuid"));
  private static final Meter persistMessageMeter = metricRegistry.meter(name(MessagesManager.class, "persistMessage"));

  private final MessagesDynamoDb messagesDynamoDb;
  private final MessagesCache messagesCache;
  private final ReportMessageManager reportMessageManager;

  public MessagesManager(
      final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final ReportMessageManager reportMessageManager) {
    this.messagesDynamoDb = messagesDynamoDb;
    this.messagesCache = messagesCache;
    this.reportMessageManager = reportMessageManager;
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

  public Pair<List<Envelope>, Boolean> getMessagesForDevice(UUID destinationUuid, long destinationDevice, final boolean cachedMessagesOnly) {
    List<Envelope> messageList = new ArrayList<>();

    if (!cachedMessagesOnly) {
      messageList.addAll(messagesDynamoDb.load(destinationUuid, destinationDevice, RESULT_SET_CHUNK_SIZE));
    }

    if (messageList.size() < RESULT_SET_CHUNK_SIZE) {
      messageList.addAll(messagesCache.get(destinationUuid, destinationDevice, RESULT_SET_CHUNK_SIZE - messageList.size()));
    }

    return new Pair<>(messageList, messageList.size() >= RESULT_SET_CHUNK_SIZE);
  }

  public void clear(UUID destinationUuid) {
    messagesCache.clear(destinationUuid);
    messagesDynamoDb.deleteAllMessagesForAccount(destinationUuid);
  }

  public void clear(UUID destinationUuid, long deviceId) {
    messagesCache.clear(destinationUuid, deviceId);
    messagesDynamoDb.deleteAllMessagesForDevice(destinationUuid, deviceId);
  }

  public Optional<Envelope> delete(UUID destinationUuid, long destinationDeviceId, UUID guid, Long serverTimestamp) {
    Optional<Envelope> removed = messagesCache.remove(destinationUuid, destinationDeviceId, guid);

    if (removed.isEmpty()) {
      if (serverTimestamp == null) {
        removed = messagesDynamoDb.deleteMessageByDestinationAndGuid(destinationUuid, guid);
      } else {
        removed = messagesDynamoDb.deleteMessage(destinationUuid, destinationDeviceId, guid, serverTimestamp);
      }
      cacheMissByGuidMeter.mark();
    } else {
      cacheHitByGuidMeter.mark();
    }

    return removed;
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
    int messagesRemovedFromCache = messagesCache.remove(destinationUuid, destinationDeviceId, messageGuids).size();

    persistMessageMeter.mark(nonEphemeralMessages.size());

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
