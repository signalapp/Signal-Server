/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;


import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class MessagesManager {

  private static final MetricRegistry metricRegistry       = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          cacheHitByIdMeter    = metricRegistry.meter(name(MessagesManager.class, "cacheHitById"   ));
  private static final Meter          cacheMissByIdMeter   = metricRegistry.meter(name(MessagesManager.class, "cacheMissById"  ));
  private static final Meter          cacheHitByNameMeter  = metricRegistry.meter(name(MessagesManager.class, "cacheHitByName" ));
  private static final Meter          cacheMissByNameMeter = metricRegistry.meter(name(MessagesManager.class, "cacheMissByName"));
  private static final Meter          cacheHitByGuidMeter  = metricRegistry.meter(name(MessagesManager.class, "cacheHitByGuid" ));
  private static final Meter          cacheMissByGuidMeter = metricRegistry.meter(name(MessagesManager.class, "cacheMissByGuid"));

  private final Messages           messages;
  private final MessagesCache      messagesCache;
  private final PushLatencyManager pushLatencyManager;

  public MessagesManager(Messages messages, MessagesCache messagesCache, PushLatencyManager pushLatencyManager) {
    this.messages           = messages;
    this.messagesCache      = messagesCache;
    this.pushLatencyManager = pushLatencyManager;
  }

  public void insert(UUID destinationUuid, long destinationDevice, Envelope message) {
    messagesCache.insert(UUID.randomUUID(), destinationUuid, destinationDevice, message);
  }

  public void insertEphemeral(final UUID destinationUuid, final long destinationDevice, final Envelope message) {
    messagesCache.insertEphemeral(destinationUuid, destinationDevice, message);
  }

  public Optional<Envelope> takeEphemeralMessage(final UUID destinationUuid, final long destinationDevice) {
    return messagesCache.takeEphemeralMessage(destinationUuid, destinationDevice);
  }

  public boolean hasCachedMessages(final UUID destinationUuid, final long destinationDevice) {
    return messagesCache.hasMessages(destinationUuid, destinationDevice);
  }

  public OutgoingMessageEntityList getMessagesForDevice(String destination, UUID destinationUuid, long destinationDevice, final String userAgent, final boolean cachedMessagesOnly) {
    RedisOperation.unchecked(() -> pushLatencyManager.recordQueueRead(destinationUuid, destinationDevice, userAgent));

    List<OutgoingMessageEntity> messages = cachedMessagesOnly ? new ArrayList<>() : this.messages.load(destination, destinationDevice);

    if (messages.size() < Messages.RESULT_SET_CHUNK_SIZE) {
      messages.addAll(messagesCache.get(destinationUuid, destinationDevice, Messages.RESULT_SET_CHUNK_SIZE - messages.size()));
    }

    return new OutgoingMessageEntityList(messages, messages.size() >= Messages.RESULT_SET_CHUNK_SIZE);
  }

  public void clear(String destination, UUID destinationUuid) {
    // TODO Remove this null check in a fully-UUID-ified world
    if (destinationUuid != null) {
      this.messagesCache.clear(destinationUuid);
    }

    this.messages.clear(destination);
  }

  public void clear(String destination, UUID destinationUuid, long deviceId) {
    // TODO Remove this null check in a fully-UUID-ified world
    if (destinationUuid != null) {
      this.messagesCache.clear(destinationUuid, deviceId);
    }

    this.messages.clear(destination, deviceId);
  }

  public Optional<OutgoingMessageEntity> delete(String destination, UUID destinationUuid, long destinationDevice, String source, long timestamp)
  {
    Optional<OutgoingMessageEntity> removed = messagesCache.remove(destinationUuid, destinationDevice, source, timestamp);

    if (!removed.isPresent()) {
      removed = this.messages.remove(destination, destinationDevice, source, timestamp);
      cacheMissByNameMeter.mark();
    } else {
      cacheHitByNameMeter.mark();
    }

    return removed;
  }

  public Optional<OutgoingMessageEntity> delete(String destination, UUID destinationUuid, long deviceId, UUID guid) {
    Optional<OutgoingMessageEntity> removed = messagesCache.remove(destinationUuid, deviceId, guid);

    if (!removed.isPresent()) {
      removed = this.messages.remove(destination, guid);
      cacheMissByGuidMeter.mark();
    } else {
      cacheHitByGuidMeter.mark();
    }

    return removed;
  }

  public void delete(String destination, UUID destinationUuid, long deviceId, long id, boolean cached) {
    if (cached) {
      messagesCache.remove(destinationUuid, deviceId, id);
      cacheHitByIdMeter.mark();
    } else {
      this.messages.remove(destination, id);
      cacheMissByIdMeter.mark();
    }
  }

  public void persistMessages(final String destination, final UUID destinationUuid, final long destinationDeviceId, final List<Envelope> messages) {
    this.messages.store(messages, destination, destinationDeviceId);
    messagesCache.remove(destinationUuid, destinationDeviceId, messages.stream().map(message -> UUID.fromString(message.getServerGuid())).collect(Collectors.toList()));
  }

  public void addMessageAvailabilityListener(final UUID destinationUuid, final long deviceId, final MessageAvailabilityListener listener) {
    messagesCache.addMessageAvailabilityListener(destinationUuid, deviceId, listener);
  }

  public void removeMessageAvailabilityListener(final MessageAvailabilityListener listener) {
    messagesCache.removeMessageAvailabilityListener(listener);
  }
}
