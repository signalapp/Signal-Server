package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.util.SafeEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class MessagesCache implements UserMessagesCache {

  private static final Logger         logger            = LoggerFactory.getLogger(MessagesCache.class);

  private static final MetricRegistry metricRegistry    = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          insertTimer       = metricRegistry.timer(name(MessagesCache.class, "insert"      ));
  private static final Timer          removeByIdTimer   = metricRegistry.timer(name(MessagesCache.class, "removeById"  ));
  private static final Timer          removeByNameTimer = metricRegistry.timer(name(MessagesCache.class, "removeByName"));
  private static final Timer          removeByGuidTimer = metricRegistry.timer(name(MessagesCache.class, "removeByGuid"));
  private static final Timer          getTimer          = metricRegistry.timer(name(MessagesCache.class, "get"         ));
  private static final Timer          clearAccountTimer = metricRegistry.timer(name(MessagesCache.class, "clearAccount"));
  private static final Timer          clearDeviceTimer  = metricRegistry.timer(name(MessagesCache.class, "clearDevice" ));

  private final ReplicatedJedisPool jedisPool;

  private final InsertOperation insertOperation;
  private final RemoveOperation removeOperation;
  private final GetOperation    getOperation;

  public MessagesCache(ReplicatedJedisPool jedisPool) throws IOException {
    this.jedisPool        = jedisPool;

    this.insertOperation  = new InsertOperation(jedisPool);
    this.removeOperation  = new RemoveOperation(jedisPool);
    this.getOperation     = new GetOperation(jedisPool);
  }

  @Override
  public long insert(UUID guid, String destination, final UUID destinationUuid, long destinationDevice, Envelope message) {
    final Envelope messageWithGuid = message.toBuilder().setServerGuid(guid.toString()).build();

    Timer.Context timer = insertTimer.time();

    try {
      return insertOperation.insert(guid, destination, destinationDevice, System.currentTimeMillis(), messageWithGuid);
    } finally {
      timer.stop();
    }
  }

  @Override
  public Optional<OutgoingMessageEntity> remove(String destination, final UUID destinationUuid, long destinationDevice, long id) {
    OutgoingMessageEntity removedMessageEntity = null;

    try (Jedis         jedis   = jedisPool.getWriteResource();
         Timer.Context ignored = removeByIdTimer.time())
    {
      byte[] serialized = removeOperation.remove(jedis, destination, destinationDevice, id);

      if (serialized != null) {
        removedMessageEntity = UserMessagesCache.constructEntityFromEnvelope(id, Envelope.parseFrom(serialized));
      }
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Failed to parse envelope", e);
    }

    return Optional.ofNullable(removedMessageEntity);
  }

  @Override
  public Optional<OutgoingMessageEntity> remove(String destination, final UUID destinationUuid, long destinationDevice, String sender, long timestamp) {
    OutgoingMessageEntity removedMessageEntity = null;
    Timer.Context timer = removeByNameTimer.time();

    try {
      byte[] serialized = removeOperation.remove(destination, destinationDevice, sender, timestamp);

      if (serialized != null) {
        removedMessageEntity = UserMessagesCache.constructEntityFromEnvelope(0, Envelope.parseFrom(serialized));
      }
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Failed to parse envelope", e);
    } finally {
      timer.stop();
    }

    return Optional.ofNullable(removedMessageEntity);
  }

  @Override
  public Optional<OutgoingMessageEntity> remove(String destination, final UUID destinationUuid, long destinationDevice, UUID guid) {
    OutgoingMessageEntity removedMessageEntity = null;
    Timer.Context timer = removeByGuidTimer.time();

    try {
      byte[] serialized = removeOperation.remove(destination, destinationDevice, guid);

      if (serialized != null) {
        removedMessageEntity = UserMessagesCache.constructEntityFromEnvelope(0, Envelope.parseFrom(serialized));
      }
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Failed to parse envelope", e);
    } finally {
      timer.stop();
    }

    return Optional.ofNullable(removedMessageEntity);
  }

  @Override
  public List<OutgoingMessageEntity> get(String destination, final UUID destinationUuid, long destinationDevice, int limit) {
    Timer.Context timer = getTimer.time();

    try {
      List<OutgoingMessageEntity> results = new LinkedList<>();
      Key                         key     = new Key(destination, destinationDevice);
      List<Pair<byte[], Double>>  items   = getOperation.getItems(key.getUserMessageQueue(), key.getUserMessageQueuePersistInProgress(), limit);

      for (Pair<byte[], Double> item : items) {
        try {
          long     id      = item.second().longValue();
          Envelope message = Envelope.parseFrom(item.first());
          results.add(UserMessagesCache.constructEntityFromEnvelope(id, message));
        } catch (InvalidProtocolBufferException e) {
          logger.warn("Failed to parse envelope", e);
        }
      }

      return results;
    } finally {
      timer.stop();
    }
  }

  @Override
  public void clear(String destination, final UUID destinationUuid) {
    Timer.Context timer = clearAccountTimer.time();

    try {
      for (int i = 1; i < 255; i++) {
        clear(destination, destinationUuid, i);
      }
    } finally {
      timer.stop();
    }
  }

  @Override
  public void clear(String destination, final UUID destinationUuid, long deviceId) {
    Timer.Context timer = clearDeviceTimer.time();

    try {
      removeOperation.clear(destination, deviceId);
    } finally {
      timer.stop();
    }
  }

  private static class InsertOperation {
    private final LuaScript insert;

    InsertOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.insert = LuaScript.fromResource(jedisPool, "lua/insert_item.lua");
    }

    public long insert(UUID guid, String destination, long destinationDevice, long timestamp, Envelope message) {
      Key    key    = new Key(destination, destinationDevice);
      String sender = message.hasSource() ? (message.getSource() + "::" + message.getTimestamp()) : "nil";

      List<byte[]> keys = Arrays.asList(key.getUserMessageQueue(), key.getUserMessageQueueMetadata(), Key.getUserMessageQueueIndex());
      List<byte[]> args = Arrays.asList(message.toByteArray(), String.valueOf(timestamp).getBytes(), sender.getBytes(), guid.toString().getBytes());

      return (long)insert.execute(keys, args);
    }
  }

  private static class RemoveOperation {

    private final LuaScript removeById;
    private final LuaScript removeBySender;
    private final LuaScript removeByGuid;
    private final LuaScript removeQueue;

    RemoveOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.removeById     = LuaScript.fromResource(jedisPool, "lua/remove_item_by_id.lua"    );
      this.removeBySender = LuaScript.fromResource(jedisPool, "lua/remove_item_by_sender.lua");
      this.removeByGuid   = LuaScript.fromResource(jedisPool, "lua/remove_item_by_guid.lua"  );
      this.removeQueue    = LuaScript.fromResource(jedisPool, "lua/remove_queue.lua"         );
    }

    public byte[] remove(Jedis jedis, String destination, long destinationDevice, long id) {
      Key key = new Key(destination, destinationDevice);

      List<byte[]> keys = Arrays.asList(key.getUserMessageQueue(), key.getUserMessageQueueMetadata(), Key.getUserMessageQueueIndex());
      List<byte[]> args = Collections.singletonList(String.valueOf(id).getBytes());

      return (byte[])this.removeById.execute(jedis, keys, args);
    }

    public byte[] remove(String destination, long destinationDevice, String sender, long timestamp) {
      Key    key       = new Key(destination, destinationDevice);
      String senderKey = sender + "::" + timestamp;

      List<byte[]> keys = Arrays.asList(key.getUserMessageQueue(), key.getUserMessageQueueMetadata(), Key.getUserMessageQueueIndex());
      List<byte[]> args = Collections.singletonList(senderKey.getBytes());

      return (byte[])this.removeBySender.execute(keys, args);
    }

    public byte[] remove(String destination, long destinationDevice, UUID guid) {
      Key key = new Key(destination, destinationDevice);

      List<byte[]> keys = Arrays.asList(key.getUserMessageQueue(), key.getUserMessageQueueMetadata(), Key.getUserMessageQueueIndex());
      List<byte[]> args = Collections.singletonList(guid.toString().getBytes());

      return (byte[])this.removeByGuid.execute(keys, args);
    }

    public void clear(String destination, long deviceId) {
      Key key = new Key(destination, deviceId);

      List<byte[]> keys = Arrays.asList(key.getUserMessageQueue(), key.getUserMessageQueueMetadata(), Key.getUserMessageQueueIndex());
      List<byte[]> args = new LinkedList<>();

      this.removeQueue.execute(keys, args);
    }
  }

  private static class GetOperation {

    private final LuaScript getItems;

    GetOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.getItems  = LuaScript.fromResource(jedisPool, "lua/get_items.lua");
    }

    List<Pair<byte[], Double>> getItems(byte[] queue, byte[] lock, int limit) {
      List<byte[]> keys = Arrays.asList(queue, lock);
      List<byte[]> args = Collections.singletonList(String.valueOf(limit).getBytes());

      Iterator<byte[]>           results = ((List<byte[]>) getItems.execute(keys, args)).iterator();
      List<Pair<byte[], Double>> items   = new LinkedList<>();

      while (results.hasNext()) {
        items.add(new Pair<>(results.next(), Double.valueOf(SafeEncoder.encode(results.next()))));
      }

      return items;
    }
  }

}
