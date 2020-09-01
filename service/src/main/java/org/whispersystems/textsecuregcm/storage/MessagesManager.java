package org.whispersystems.textsecuregcm.storage;


import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.experiment.Experiment;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class MessagesManager {

  private static final MetricRegistry metricRegistry       = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          cacheHitByIdMeter    = metricRegistry.meter(name(MessagesManager.class, "cacheHitById"   ));
  private static final Meter          cacheMissByIdMeter   = metricRegistry.meter(name(MessagesManager.class, "cacheMissById"  ));
  private static final Meter          cacheHitByNameMeter  = metricRegistry.meter(name(MessagesManager.class, "cacheHitByName" ));
  private static final Meter          cacheMissByNameMeter = metricRegistry.meter(name(MessagesManager.class, "cacheMissByName"));
  private static final Meter          cacheHitByGuidMeter  = metricRegistry.meter(name(MessagesManager.class, "cacheHitByGuid" ));
  private static final Meter          cacheMissByGuidMeter = metricRegistry.meter(name(MessagesManager.class, "cacheMissByGuid"));

  private final Messages                  messages;
  private final MessagesCache             messagesCache;
  private final RedisClusterMessagesCache clusterMessagesCache;
  private final PushLatencyManager        pushLatencyManager;

  private final Experiment      insertExperiment         = new Experiment("MessagesCache", "insert");
  private final Experiment      removeByIdExperiment     = new Experiment("MessagesCache", "removeById");
  private final Experiment      removeBySenderExperiment = new Experiment("MessagesCache", "removeBySender");
  private final Experiment      removeByUuidExperiment   = new Experiment("MessagesCache", "removeByUuid");
  private final Experiment      getMessagesExperiment    = new Experiment("MessagesCache", "getMessages");

  public MessagesManager(Messages messages, MessagesCache messagesCache, RedisClusterMessagesCache clusterMessagesCache, PushLatencyManager pushLatencyManager) {
    this.messages             = messages;
    this.messagesCache        = messagesCache;
    this.clusterMessagesCache = clusterMessagesCache;
    this.pushLatencyManager   = pushLatencyManager;
  }

  public void insert(String destination, UUID destinationUuid, long destinationDevice, Envelope message) {
    final UUID guid      = UUID.randomUUID();
    final long messageId = messagesCache.insert(guid, destination, destinationUuid, destinationDevice, message);

    insertExperiment.compareSupplierResult(messageId, () -> clusterMessagesCache.insert(guid, destination, destinationUuid, destinationDevice, message, messageId));
  }

  public OutgoingMessageEntityList getMessagesForDevice(String destination, UUID destinationUuid, long destinationDevice, final String userAgent) {
    RedisOperation.unchecked(() -> pushLatencyManager.recordQueueRead(destinationUuid, destinationDevice, userAgent));

    List<OutgoingMessageEntity> messages = this.messages.load(destination, destinationDevice);

    if (messages.size() <= Messages.RESULT_SET_CHUNK_SIZE) {
      final List<OutgoingMessageEntity> messagesFromCache = this.messagesCache.get(destination, destinationUuid, destinationDevice, Messages.RESULT_SET_CHUNK_SIZE - messages.size());
      getMessagesExperiment.compareSupplierResult(messagesFromCache, () -> clusterMessagesCache.get(destination, destinationUuid, destinationDevice, Messages.RESULT_SET_CHUNK_SIZE - messages.size()));

      messages.addAll(messagesFromCache);
    }

    return new OutgoingMessageEntityList(messages, messages.size() >= Messages.RESULT_SET_CHUNK_SIZE);
  }

  public void clear(String destination, UUID destinationUuid) {
    this.messagesCache.clear(destination, destinationUuid);
    this.clusterMessagesCache.clear(destination, destinationUuid);
    this.messages.clear(destination);
  }

  public void clear(String destination, UUID destinationUuid, long deviceId) {
    this.messagesCache.clear(destination, destinationUuid, deviceId);
    this.clusterMessagesCache.clear(destination, destinationUuid, deviceId);
    this.messages.clear(destination, deviceId);
  }

  public Optional<OutgoingMessageEntity> delete(String destination, UUID destinationUuid, long destinationDevice, String source, long timestamp)
  {
    Optional<OutgoingMessageEntity> removed = this.messagesCache.remove(destination, destinationUuid, destinationDevice, source, timestamp);
    removeBySenderExperiment.compareSupplierResult(removed, () -> clusterMessagesCache.remove(destination, destinationUuid, destinationDevice, source, timestamp));

    if (!removed.isPresent()) {
      removed = this.messages.remove(destination, destinationDevice, source, timestamp);
      cacheMissByNameMeter.mark();
    } else {
      cacheHitByNameMeter.mark();
    }

    return removed;
  }

  public Optional<OutgoingMessageEntity> delete(String destination, UUID destinationUuid, long deviceId, UUID guid) {
    Optional<OutgoingMessageEntity> removed = this.messagesCache.remove(destination, destinationUuid, deviceId, guid);
    removeByUuidExperiment.compareSupplierResult(removed, () -> clusterMessagesCache.remove(destination, destinationUuid, deviceId, guid));

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
      final Optional<OutgoingMessageEntity> maybeRemovedMessage = this.messagesCache.remove(destination, destinationUuid, deviceId, id);
      removeByIdExperiment.compareSupplierResult(maybeRemovedMessage, () -> clusterMessagesCache.remove(destination, destinationUuid, deviceId, id));
      cacheHitByIdMeter.mark();
    } else {
      this.messages.remove(destination, id);
      cacheMissByIdMeter.mark();
    }
  }

  public void persistMessage(String destination, UUID destinationUuid, Envelope envelope, UUID messageGuid, long deviceId) {
    messages.store(messageGuid, envelope, destination, deviceId);
    delete(destination, destinationUuid, deviceId, messageGuid);
  }

  public void addMessageAvailabilityListener(final UUID destinationUuid, final long deviceId, final MessageAvailabilityListener listener) {
    clusterMessagesCache.addMessageAvailabilityListener(destinationUuid, deviceId, listener);
  }

  public void removeMessageAvailabilityListener(final MessageAvailabilityListener listener) {
    clusterMessagesCache.removeMessageAvailabilityListener(listener);
  }
}
