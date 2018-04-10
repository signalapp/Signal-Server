package org.whispersystems.textsecuregcm.storage;


import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Conversions;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

public class MessagesManager {

  private static final MetricRegistry metricRegistry       = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          cacheHitByIdMeter    = metricRegistry.meter(name(MessagesManager.class, "cacheHitById"   ));
  private static final Meter          cacheMissByIdMeter   = metricRegistry.meter(name(MessagesManager.class, "cacheMissById"  ));
  private static final Meter          cacheHitByNameMeter  = metricRegistry.meter(name(MessagesManager.class, "cacheHitByName" ));
  private static final Meter          cacheMissByNameMeter = metricRegistry.meter(name(MessagesManager.class, "cacheMissByName"));

  private final Messages      messages;
  private final MessagesCache messagesCache;
  private final Distribution  distribution;

  public MessagesManager(Messages messages, MessagesCache messagesCache, float cacheRate) {
    this.messages      = messages;
    this.messagesCache = messagesCache;
    this.distribution  = new Distribution(cacheRate);
  }

  public void insert(String destination, long destinationDevice, Envelope message) {
    if (distribution.isQualified(destination, destinationDevice)) {
      messagesCache.insert(destination, destinationDevice, message);
    } else {
      messages.store(message, destination, destinationDevice);
    }
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
      removed = Optional.fromNullable(this.messages.remove(destination, destinationDevice, source, timestamp));
      cacheMissByNameMeter.mark();
    } else {
      cacheHitByNameMeter.mark();
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

  public static class Distribution {

    private final float percentage;

    public Distribution(float percentage) {
      this.percentage = percentage;
    }

    public boolean isQualified(String address, long device) {
      if (percentage <= 0)   return false;
      if (percentage >= 100) return true;

      try {
        MessageDigest digest = MessageDigest.getInstance("SHA1");
        digest.update(address.getBytes());
        digest.update(Conversions.longToByteArray(device));

        byte[] result = digest.digest();
        int hashCode = Conversions.byteArrayToShort(result);

        return hashCode <= 65535 * percentage;
      } catch (NoSuchAlgorithmException e) {
        throw new AssertionError(e);
      }
    }

  }

}
