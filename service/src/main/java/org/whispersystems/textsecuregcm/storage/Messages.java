package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.storage.mappers.OutgoingMessageEntityRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class Messages {

  static final int RESULT_SET_CHUNK_SIZE = 100;

  public static final String ID                 = "id";
  public static final String GUID               = "guid";
  public static final String TYPE               = "type";
  public static final String RELAY              = "relay";
  public static final String TIMESTAMP          = "timestamp";
  public static final String SERVER_TIMESTAMP   = "server_timestamp";
  public static final String SOURCE             = "source";
  public static final String SOURCE_UUID        = "source_uuid";
  public static final String SOURCE_DEVICE      = "source_device";
  public static final String DESTINATION        = "destination";
  public static final String DESTINATION_DEVICE = "destination_device";
  public static final String MESSAGE            = "message";
  public static final String CONTENT            = "content";

  private final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          storeTimer          = metricRegistry.timer(name(Messages.class, "store"         ));
  private final Timer          loadTimer           = metricRegistry.timer(name(Messages.class, "load"          ));
  private final Timer          removeBySourceTimer = metricRegistry.timer(name(Messages.class, "removeBySource"));
  private final Timer          removeByGuidTimer   = metricRegistry.timer(name(Messages.class, "removeByGuid"  ));
  private final Timer          removeByIdTimer     = metricRegistry.timer(name(Messages.class, "removeById"    ));
  private final Timer          clearDeviceTimer    = metricRegistry.timer(name(Messages.class, "clearDevice"   ));
  private final Timer          clearTimer          = metricRegistry.timer(name(Messages.class, "clear"         ));
  private final Timer          vacuumTimer         = metricRegistry.timer(name(Messages.class, "vacuum"));

  private final FaultTolerantDatabase database;

  public Messages(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new OutgoingMessageEntityRowMapper());
  }

  public void store(UUID guid, Envelope message, String destination, long destinationDevice) {
    database.use(jdbi ->jdbi.useHandle(handle -> {
      try (Timer.Context ignored = storeTimer.time()) {
        handle.createUpdate("INSERT INTO messages (" + GUID + ", " + TYPE + ", " + RELAY + ", " + TIMESTAMP + ", " + SERVER_TIMESTAMP + ", " + SOURCE + ", " + SOURCE_UUID + ", " + SOURCE_DEVICE + ", " + DESTINATION + ", " + DESTINATION_DEVICE + ", " + MESSAGE + ", " + CONTENT + ") " +
                                "VALUES (:guid, :type, :relay, :timestamp, :server_timestamp, :source, :source_uuid, :source_device, :destination, :destination_device, :message, :content)")
              .bind("guid", guid)
              .bind("destination", destination)
              .bind("destination_device", destinationDevice)
              .bind("type", message.getType().getNumber())
              .bind("relay", message.getRelay())
              .bind("timestamp", message.getTimestamp())
              .bind("server_timestamp", message.getServerTimestamp())
              .bind("source", message.hasSource() ? message.getSource() : null)
              .bind("source_uuid", message.hasSourceUuid() ? UUID.fromString(message.getSourceUuid()) : null)
              .bind("source_device", message.hasSourceDevice() ? message.getSourceDevice() : null)
              .bind("message", message.hasLegacyMessage() ? message.getLegacyMessage().toByteArray() : null)
              .bind("content", message.hasContent() ? message.getContent().toByteArray() : null)
              .execute();
      }
    }));
  }

  public List<OutgoingMessageEntity> load(String destination, long destinationDevice) {
    return database.with(jdbi-> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = loadTimer.time()) {
        return handle.createQuery("SELECT * FROM messages WHERE " + DESTINATION + " = :destination AND " + DESTINATION_DEVICE + " = :destination_device ORDER BY " + TIMESTAMP + " ASC LIMIT " + RESULT_SET_CHUNK_SIZE)
                     .bind("destination", destination)
                     .bind("destination_device", destinationDevice)
                     .mapTo(OutgoingMessageEntity.class)
                     .list();
      }
    }));
  }

  public Optional<OutgoingMessageEntity> remove(String destination, long destinationDevice, String source, long timestamp) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = removeBySourceTimer.time()) {
        return handle.createQuery("DELETE FROM messages WHERE " + ID + " IN (SELECT " + ID + " FROM messages WHERE " + DESTINATION + " = :destination AND " + DESTINATION_DEVICE + " = :destination_device AND " + SOURCE + " = :source AND " + TIMESTAMP + " = :timestamp ORDER BY " + ID + " LIMIT 1) RETURNING *")
                     .bind("destination", destination)
                     .bind("destination_device", destinationDevice)
                     .bind("source", source)
                     .bind("timestamp", timestamp)
                     .mapTo(OutgoingMessageEntity.class)
                     .findFirst();
      }
    }));
  }

  public Optional<OutgoingMessageEntity> remove(String destination, UUID guid) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = removeByGuidTimer.time()) {
        return handle.createQuery("DELETE FROM messages WHERE " + ID + " IN (SELECT " + ID + " FROM MESSAGES WHERE " + GUID + " = :guid AND " + DESTINATION + " = :destination ORDER BY " + ID + " LIMIT 1) RETURNING *")
                     .bind("destination", destination)
                     .bind("guid", guid)
                     .mapTo(OutgoingMessageEntity.class)
                     .findFirst();
      }
    }));
  }

  public void remove(String destination, long id) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = removeByIdTimer.time()) {
        handle.createUpdate("DELETE FROM messages WHERE " + ID + " = :id AND " + DESTINATION + " = :destination")
              .bind("destination", destination)
              .bind("id", id)
              .execute();
      }
    }));
  }

  public void clear(String destination) {
    database.use(jdbi ->jdbi.useHandle(handle -> {
      try (Timer.Context ignored = clearTimer.time()) {
        handle.createUpdate("DELETE FROM messages WHERE " + DESTINATION + " = :destination")
              .bind("destination", destination)
              .execute();
      }
    }));
  }

  public void clear(String destination, long destinationDevice) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = clearDeviceTimer.time()) {
        handle.createUpdate("DELETE FROM messages WHERE " + DESTINATION + " = :destination AND " + DESTINATION_DEVICE + " = :destination_device")
              .bind("destination", destination)
              .bind("destination_device", destinationDevice)
              .execute();
      }
    }));
  }

  public void vacuum() {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = vacuumTimer.time()) {
        handle.execute("VACUUM messages");
      }
    }));
  }


}
