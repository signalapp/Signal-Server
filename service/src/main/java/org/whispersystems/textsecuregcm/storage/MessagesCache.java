package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.experiment.Experiment;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.util.SafeEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;

public class MessagesCache implements Managed, UserMessagesCache {

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
  private final Messages            database;
  private final AccountsManager     accountsManager;
  private final int                 delayMinutes;

  private final InsertOperation insertOperation;
  private final RemoveOperation removeOperation;
  private final GetOperation    getOperation;

  private PubSubManager    pubSubManager;
  private PushSender       pushSender;
  private MessagePersister messagePersister;

  private final RedisClusterMessagesCache clusterMessagesCache;
  private final ExecutorService           experimentExecutor = new ThreadPoolExecutor(8, 8, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1_000));

  private final Experiment insertExperiment         = new Experiment("MessagesCache", "insert");
  private final Experiment removeByIdExperiment     = new Experiment("MessagesCache", "removeById");
  private final Experiment removeBySenderExperiment = new Experiment("MessagesCache", "removeBySender");
  private final Experiment removeByUuidExperiment   = new Experiment("MessagesCache", "removeByUuid");
  private final Experiment getMessagesExperiment    = new Experiment("MessagesCache", "getMessages");

  public MessagesCache(ReplicatedJedisPool jedisPool, Messages database, AccountsManager accountsManager, int delayMinutes, RedisClusterMessagesCache clusterMessagesCache) throws IOException {
    this.jedisPool        = jedisPool;
    this.database         = database;
    this.accountsManager  = accountsManager;
    this.delayMinutes     = delayMinutes;

    this.insertOperation  = new InsertOperation(jedisPool);
    this.removeOperation  = new RemoveOperation(jedisPool);
    this.getOperation     = new GetOperation(jedisPool);

    this.clusterMessagesCache = clusterMessagesCache;
  }

  @Override
  public long insert(UUID guid, String destination, final UUID destinationUuid, long destinationDevice, Envelope message) {
    final Envelope messageWithGuid = message.toBuilder().setServerGuid(guid.toString()).build();

    Timer.Context timer = insertTimer.time();

    try {
      final long messageId = insertOperation.insert(guid, destination, destinationDevice, System.currentTimeMillis(), messageWithGuid);
      insertExperiment.compareSupplierResultAsync(messageId, () -> clusterMessagesCache.insert(guid, destination, destinationUuid, destinationDevice, message, messageId), experimentExecutor);

      return messageId;
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

    final Optional<OutgoingMessageEntity> maybeRemovedMessage = Optional.ofNullable(removedMessageEntity);

    removeByIdExperiment.compareSupplierResultAsync(maybeRemovedMessage, () -> clusterMessagesCache.remove(destination, destinationUuid, destinationDevice, id), experimentExecutor);

    return maybeRemovedMessage;
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

    final Optional<OutgoingMessageEntity> maybeRemovedMessage = Optional.ofNullable(removedMessageEntity);

    removeBySenderExperiment.compareSupplierResultAsync(maybeRemovedMessage, () -> clusterMessagesCache.remove(destination, destinationUuid, destinationDevice, sender, timestamp), experimentExecutor);

    return maybeRemovedMessage;
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

    final Optional<OutgoingMessageEntity> maybeRemovedMessage = Optional.ofNullable(removedMessageEntity);

    removeByUuidExperiment.compareSupplierResultAsync(maybeRemovedMessage, () -> clusterMessagesCache.remove(destination, destinationUuid, destinationDevice, guid), experimentExecutor);

    return maybeRemovedMessage;
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

      getMessagesExperiment.compareSupplierResultAsync(results, () -> clusterMessagesCache.get(destination, destinationUuid, destinationDevice, limit), experimentExecutor);

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

  public void setPubSubManager(PubSubManager pubSubManager, PushSender pushSender) {
    this.pubSubManager = pubSubManager;
    this.pushSender    = pushSender;
  }

  @Override
  public void start() throws Exception {
    this.messagePersister = new MessagePersister(jedisPool, database, pubSubManager, pushSender, accountsManager, delayMinutes, TimeUnit.MINUTES);
    this.messagePersister.start();
  }

  @Override
  public void stop() throws Exception {
    messagePersister.shutdown();
    logger.info("Message persister shut down...");

    this.experimentExecutor.shutdown();
  }

  private static class Key {

    private final byte[] userMessageQueue;
    private final byte[] userMessageQueueMetadata;
    private final byte[] userMessageQueuePersistInProgress;

    private final String address;
    private final long   deviceId;

    Key(String address, long deviceId) {
      this.address                           = address;
      this.deviceId                          = deviceId;
      this.userMessageQueue                  = ("user_queue::" + address + "::" + deviceId).getBytes();
      this.userMessageQueueMetadata          = ("user_queue_metadata::" + address + "::" + deviceId).getBytes();
      this.userMessageQueuePersistInProgress = ("user_queue_persisting::" + address + "::" + deviceId).getBytes();
    }

    String getAddress() {
      return address;
    }

    long getDeviceId() {
      return deviceId;
    }

    byte[] getUserMessageQueue() {
      return userMessageQueue;
    }

    byte[] getUserMessageQueueMetadata() {
      return userMessageQueueMetadata;
    }

    byte[] getUserMessageQueuePersistInProgress() {
      return userMessageQueuePersistInProgress;
    }

    static byte[] getUserMessageQueueIndex() {
      return "user_queue_index".getBytes();
    }

    static Key fromUserMessageQueue(byte[] userMessageQueue) throws IOException {
      try {
        String[] parts = new String(userMessageQueue).split("::");

        if (parts.length != 3) {
          throw new IOException("Malformed key: " + new String(userMessageQueue));
        }

        return new Key(parts[1], Long.parseLong(parts[2]));
      } catch (NumberFormatException e) {
        throw new IOException(e);
      }
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

    private final LuaScript getQueues;
    private final LuaScript getItems;

    GetOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.getQueues = LuaScript.fromResource(jedisPool, "lua/get_queues_to_persist.lua");
      this.getItems  = LuaScript.fromResource(jedisPool, "lua/get_items.lua");
    }

    List<byte[]> getQueues(byte[] queue, long maxTimeMillis, int limit) {
      List<byte[]> keys = Collections.singletonList(queue);
      List<byte[]> args = Arrays.asList(String.valueOf(maxTimeMillis).getBytes(), String.valueOf(limit).getBytes());

      return (List<byte[]>)getQueues.execute(keys, args);
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

  private class MessagePersister extends Thread {

    private final Logger         logger              = LoggerFactory.getLogger(MessagePersister.class);
    private final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    private final Timer          getQueuesTimer      = metricRegistry.timer(name(MessagesCache.class, "getQueues"   ));
    private final Timer          persistQueueTimer   = metricRegistry.timer(name(MessagesCache.class, "persistQueue"));
    private final Timer          notifyTimer         = metricRegistry.timer(name(MessagesCache.class, "notifyUser"  ));
    private final Histogram      queueSizeHistogram  = metricRegistry.histogram(name(MessagesCache.class, "persistQueueSize" ));
    private final Histogram      queueCountHistogram = metricRegistry.histogram(name(MessagesCache.class, "persistQueueCount"));

    private static final int CHUNK_SIZE = 100;

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final ReplicatedJedisPool jedisPool;
    private final Messages            database;
    private final long                delayTime;
    private final TimeUnit            delayTimeUnit;

    private final PubSubManager   pubSubManager;
    private final PushSender      pushSender;
    private final AccountsManager accountsManager;

    private final GetOperation    getOperation;
    private final RemoveOperation removeOperation;

    private boolean finished = false;

    MessagePersister(ReplicatedJedisPool jedisPool,
                     Messages            database,
                     PubSubManager       pubSubManager,
                     PushSender          pushSender,
                     AccountsManager     accountsManager,
                     long                delayTime,
                     TimeUnit            delayTimeUnit)
        throws IOException
    {
      super(MessagePersister.class.getSimpleName());
      this.jedisPool = jedisPool;
      this.database  = database;

      this.pubSubManager   = pubSubManager;
      this.pushSender      = pushSender;
      this.accountsManager = accountsManager;

      this.delayTime       = delayTime;
      this.delayTimeUnit   = delayTimeUnit;
      this.getOperation    = new GetOperation(jedisPool);
      this.removeOperation = new RemoveOperation(jedisPool);
    }

    @Override
    public void run() {
      while (running.get()) {
        try {
          List<byte[]> queuesToPersist = getQueuesToPersist(getOperation);
          queueCountHistogram.update(queuesToPersist.size());

          for (byte[] queue : queuesToPersist) {
            Key key = Key.fromUserMessageQueue(queue);

            persistQueue(jedisPool, key);
            notifyClients(accountsManager, pubSubManager, pushSender, key);
          }

          if (queuesToPersist.isEmpty()) {
            Thread.sleep(10000);
          }
        } catch (Throwable t) {
          logger.error("Exception while persisting: ", t);
        }
      }

      synchronized (this) {
        finished = true;
        notifyAll();
      }
    }

    synchronized void shutdown() {
      running.set(false);
      while (!finished) Util.wait(this);
    }

    private void persistQueue(ReplicatedJedisPool jedisPool, Key key) throws IOException {
      Timer.Context timer = persistQueueTimer.time();

      int messagesPersistedCount = 0;

      UUID destinationUuid = accountsManager.get(key.getAddress()).map(Account::getUuid).orElse(null);

      try (Jedis jedis = jedisPool.getWriteResource()) {
        while (true) {
          jedis.setex(key.getUserMessageQueuePersistInProgress(), 30, "1".getBytes());

          Set<Tuple> messages = jedis.zrangeWithScores(key.getUserMessageQueue(), 0, CHUNK_SIZE);

          for (Tuple message : messages) {
            persistMessage(key, destinationUuid, (long)message.getScore(), message.getBinaryElement());
            messagesPersistedCount++;
          }

          if (messages.size() < CHUNK_SIZE) {
            jedis.del(key.getUserMessageQueuePersistInProgress());
            return;
          }
        }
      } finally {
        timer.stop();
        queueSizeHistogram.update(messagesPersistedCount);
      }
    }

    private void persistMessage(Key key, UUID destinationUuid, long score, byte[] message) {
      try {
        Envelope envelope = Envelope.parseFrom(message);
        UUID     guid     = envelope.hasServerGuid() ? UUID.fromString(envelope.getServerGuid()) : null;

        envelope = envelope.toBuilder().clearServerGuid().build();

        database.store(guid, envelope, key.getAddress(), key.getDeviceId());
      } catch (InvalidProtocolBufferException e) {
        logger.error("Error parsing envelope", e);
      }

      remove(key.getAddress(), destinationUuid, key.getDeviceId(), score);
    }

    private List<byte[]> getQueuesToPersist(GetOperation getOperation) {
      Timer.Context timer = getQueuesTimer.time();
      try {
        long maxTime = System.currentTimeMillis() - delayTimeUnit.toMillis(delayTime);
        return getOperation.getQueues(Key.getUserMessageQueueIndex(), maxTime, 100);
      } finally {
        timer.stop();
      }
    }

    private void notifyClients(AccountsManager accountsManager, PubSubManager pubSubManager, PushSender pushSender, Key key) {
      Timer.Context timer = notifyTimer.time();

      try {
        boolean notified = pubSubManager.publish(new WebsocketAddress(key.getAddress(), key.getDeviceId()),
                                                 PubSubProtos.PubSubMessage.newBuilder()
                                                                           .setType(PubSubProtos.PubSubMessage.Type.QUERY_DB)
                                                                           .build());

        if (!notified) {
          Optional<Account> account = accountsManager.get(key.getAddress());

          if (account.isPresent()) {
            Optional<Device> device = account.get().getDevice(key.getDeviceId());

            if (device.isPresent()) {
              try {
                pushSender.sendQueuedNotification(account.get(), device.get());
              } catch (NotPushRegisteredException e) {
                logger.warn("After message persistence, no longer push registered!");
              }
            }
          }
        }
      } finally {
        timer.stop();
      }
    }
  }
}
