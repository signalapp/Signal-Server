package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class PubSubManager {

  private static final byte[] KEEPALIVE_CHANNEL = "KEEPALIVE".getBytes();

  private final Logger                      logger       = LoggerFactory.getLogger(PubSubManager.class);
  private final SubscriptionListener        baseListener = new SubscriptionListener();
  private final Map<String, PubSubListener> listeners    = new HashMap<>();

  private final JedisPool jedisPool;
  private boolean subscribed = false;

  public PubSubManager(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
    initializePubSubWorker();
    waitForSubscription();
  }

  public synchronized void subscribe(WebsocketAddress address, PubSubListener listener) {
    listeners.put(address.serialize(), listener);
    baseListener.subscribe(address.serialize().getBytes());
  }

  public synchronized void unsubscribe(WebsocketAddress address, PubSubListener listener) {
    if (listeners.get(address.serialize()) == listener) {
      listeners.remove(address.serialize());
      baseListener.unsubscribe(address.serialize().getBytes());
    }
  }

  public synchronized boolean publish(WebsocketAddress address, PubSubMessage message) {
    return publish(address.serialize().getBytes(), message);
  }

  private synchronized boolean publish(byte[] channel, PubSubMessage message) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.publish(channel, message.toByteArray()) != 0;
    }
  }

  private synchronized void waitForSubscription() {
    try {
      while (!subscribed) {
        wait();
      }
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  private void initializePubSubWorker() {
    new Thread("PubSubListener") {
      @Override
      public void run() {
        for (;;) {
          try (Jedis jedis = jedisPool.getResource()) {
            jedis.subscribe(baseListener, KEEPALIVE_CHANNEL);
            logger.warn("**** Unsubscribed from holding channel!!! ******");
          }
        }
      }
    }.start();

    new Thread("PubSubKeepAlive") {
      @Override
      public void run() {
        for (;;) {
          try {
            Thread.sleep(20000);
            publish(KEEPALIVE_CHANNEL, PubSubMessage.newBuilder()
                                                    .setType(PubSubMessage.Type.KEEPALIVE)
                                                    .build());
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          }
        }
      }
    }.start();
  }

  private class SubscriptionListener extends BinaryJedisPubSub {

    @Override
    public void onMessage(byte[] channel, byte[] message) {
      try {
        PubSubListener listener;

        synchronized (PubSubManager.this) {
          listener = listeners.get(channel);
        }

        if (listener != null) {
          listener.onPubSubMessage(PubSubMessage.parseFrom(message));
        }
      } catch (InvalidProtocolBufferException e) {
        logger.warn("Error parsing PubSub protobuf", e);
      }
    }

    @Override
    public void onPMessage(byte[] s, byte[] s2, byte[] s3) {
      logger.warn("Received PMessage!");
    }

    @Override
    public void onSubscribe(byte[] channel, int count) {
      if (Arrays.equals(KEEPALIVE_CHANNEL, channel)) {
        synchronized (PubSubManager.this) {
          subscribed = true;
          PubSubManager.this.notifyAll();
        }
      }
    }

    @Override
    public void onUnsubscribe(byte[] s, int i) {}

    @Override
    public void onPUnsubscribe(byte[] s, int i) {}

    @Override
    public void onPSubscribe(byte[] s, int i) {}
  }
}
