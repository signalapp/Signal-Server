package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;

public class MessagePersister implements Managed, Runnable {

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
  private final long                delayTime;
  private final TimeUnit            delayTimeUnit;

  private final MessagesManager     messagesManager;
  private final PubSubManager       pubSubManager;
  private final PushSender          pushSender;
  private final AccountsManager     accountsManager;
  private final FeatureFlagsManager featureFlagsManager;

  private final LuaScript getQueuesScript;

  private boolean finished = false;

  private static final String DISABLE_PERSISTENCE_FLAG = "disable-singleton-persister";

  public MessagePersister(final ReplicatedJedisPool jedisPool,
                          final MessagesManager messagesManager,
                          final PubSubManager pubSubManager,
                          final PushSender pushSender,
                          final AccountsManager accountsManager,
                          final FeatureFlagsManager featureFlagsManager,
                          final long delayTime,
                          final TimeUnit delayTimeUnit)
      throws IOException
  {
    this.jedisPool = jedisPool;

    this.messagesManager     = messagesManager;
    this.pubSubManager       = pubSubManager;
    this.pushSender          = pushSender;
    this.accountsManager     = accountsManager;
    this.featureFlagsManager = featureFlagsManager;

    this.delayTime       = delayTime;
    this.delayTimeUnit   = delayTimeUnit;
    this.getQueuesScript = LuaScript.fromResource(jedisPool, "lua/get_queues_to_persist.lua");
  }

  @Override
  public void start() {
    new Thread(this, getClass().getSimpleName()).start();
  }

  @Override
  public void run() {
    while (running.get()) {
      if (!featureFlagsManager.isFeatureFlagActive(DISABLE_PERSISTENCE_FLAG)) {
        try {
          List<byte[]> queuesToPersist = getQueuesToPersist();
          queueCountHistogram.update(queuesToPersist.size());

          for (byte[] queue : queuesToPersist) {
            Key key = Key.fromUserMessageQueue(queue);

            persistQueue(jedisPool, key);
            notifyClients(accountsManager, pubSubManager, pushSender, key);
          }

          if (queuesToPersist.isEmpty()) {
            //noinspection BusyWait
            Thread.sleep(10_000);
          }
        } catch (Throwable t) {
          logger.error("Exception while persisting: ", t);
        }
      } else {
        try {
          Thread.sleep(10_000);
        } catch (final InterruptedException ignored) {
        }
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  @Override
  public synchronized void stop() {
    running.set(false);
    while (!finished) Util.wait(this);

    logger.info("Message persister shut down...");
  }

  private void persistQueue(ReplicatedJedisPool jedisPool, Key key) {
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
      MessageProtos.Envelope envelope = MessageProtos.Envelope.parseFrom(message);
      UUID                   guid     = envelope.hasServerGuid() ? UUID.fromString(envelope.getServerGuid()) : null;

      envelope = envelope.toBuilder().clearServerGuid().build();

      messagesManager.persistMessage(key.getAddress(), destinationUuid, envelope, guid, key.getDeviceId(), score);
    } catch (InvalidProtocolBufferException e) {
      logger.error("Error parsing envelope", e);
    }
  }

  private List<byte[]> getQueuesToPersist() {
    Timer.Context timer = getQueuesTimer.time();
    try {
      long maxTime = System.currentTimeMillis() - delayTimeUnit.toMillis(delayTime);
      List<byte[]> keys = Collections.singletonList(Key.getUserMessageQueueIndex());
      List<byte[]> args = Arrays.asList(String.valueOf(maxTime).getBytes(), String.valueOf(100).getBytes());

      //noinspection unchecked
      return (List<byte[]>)getQueuesScript.execute(keys, args);
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
