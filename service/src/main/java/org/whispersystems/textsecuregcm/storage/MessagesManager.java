package org.whispersystems.textsecuregcm.storage;


import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
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


  private final Messages      messages;
  private final MessagesCache messagesCache;

  public MessagesManager(Messages messages, MessagesCache messagesCache) {
    this.messages      = messages;
    this.messagesCache = messagesCache;
  }

  public void insert(String destination, long destinationDevice, Envelope message) {
    UUID guid = UUID.randomUUID();
    messagesCache.insert(guid, destination, destinationDevice, message);
  }

  public OutgoingMessageEntityList getMessagesForDevice(String destination, long destinationDevice) {
    List<OutgoingMessageEntity> messages = this.messages.load(destination, destinationDevice);

    if (messages.size() <= Messages.RESULT_SET_CHUNK_SIZE) {
      messages.addAll(this.messagesCache.get(destination, destinationDevice, Messages.RESULT_SET_CHUNK_SIZE - messages.size()));
    }

    return new OutgoingMessageEntityList(messages, messages.size() >= Messages.RESULT_SET_CHUNK_SIZE);
  }

  public void clear(String destination) {
    this.messagesCache.clear(destination);
    this.messages.clear(destination);
  }

  public void clear(String destination, long deviceId) {
    this.messagesCache.clear(destination, deviceId);
    this.messages.clear(destination, deviceId);
  }

  public Optional<OutgoingMessageEntity> delete(String destination, long destinationDevice, String source, long timestamp)
  {
    Optional<OutgoingMessageEntity> removed = this.messagesCache.remove(destination, destinationDevice, source, timestamp);

    if (!removed.isPresent()) {
      removed = this.messages.remove(destination, destinationDevice, source, timestamp);
      cacheMissByNameMeter.mark();
    } else {
      cacheHitByNameMeter.mark();
    }

    return removed;
  }

  public Optional<OutgoingMessageEntity> delete(String destination, long deviceId, UUID guid) {
    Optional<OutgoingMessageEntity> removed = this.messagesCache.remove(destination, deviceId, guid);

    if (!removed.isPresent()) {
      removed = this.messages.remove(destination, guid);
      cacheMissByGuidMeter.mark();
    } else {
      cacheHitByGuidMeter.mark();
    }

    return removed;
  }

  public void delete(String destination, long deviceId, long id, boolean cached) {
    if (cached) {
      this.messagesCache.remove(destination, deviceId, id);
      cacheHitByIdMeter.mark();
    } else {
      this.messages.remove(destination, id);
      cacheMissByIdMeter.mark();
    }
  }

}
