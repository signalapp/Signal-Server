package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.websocket.InvalidWebsocketAddressException;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

public class PubSubManager {

  private static final String KEEPALIVE_CHANNEL = "KEEPALIVE";

  private final Logger                      logger       = LoggerFactory.getLogger(PubSubManager.class);
  private final ObjectMapper                mapper       = SystemMapper.getMapper();
  private final SubscriptionListener        baseListener = new SubscriptionListener();
  private final Map<String, PubSubListener> listeners    = new HashMap<>();

  private final JedisPool jedisPool;
  private boolean subscribed = false;

  public PubSubManager(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
    initializePubSubWorker();
    waitForSubscription();
  }

  public synchronized void subscribe(WebsocketAddress address, PubSubListener listener) {
    listeners.put(address.serialize(), listener);
    baseListener.subscribe(address.serialize());
  }

  public synchronized void unsubscribe(WebsocketAddress address, PubSubListener listener) {
    if (listeners.get(address.serialize()) == listener) {
      listeners.remove(address.serialize());
      baseListener.unsubscribe(address.serialize());
    }
  }

  public synchronized boolean publish(WebsocketAddress address, PubSubMessage message) {
    return publish(address.serialize(), message);
  }

  private synchronized boolean publish(String channel, PubSubMessage message) {
    try {
      String serialized = mapper.writeValueAsString(message);
      Jedis  jedis      = null;

      try {
        jedis = jedisPool.getResource();
        return jedis.publish(channel, serialized) != 0;
      } finally {
        if (jedis != null)
          jedisPool.returnResource(jedis);
      }
    } catch (JsonProcessingException e) {
      throw new AssertionError(e);
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
          Jedis jedis = null;
          try {
            jedis = jedisPool.getResource();
            jedis.subscribe(baseListener, KEEPALIVE_CHANNEL);
            logger.warn("**** Unsubscribed from holding channel!!! ******");
          } finally {
            if (jedis != null)
              jedisPool.returnResource(jedis);
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
            publish(KEEPALIVE_CHANNEL, new PubSubMessage(0, "foo"));
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          }
        }
      }
    }.start();
  }

  private class SubscriptionListener extends JedisPubSub {

    @Override
    public void onMessage(String channel, String message) {
      try {
        PubSubListener   listener;

        synchronized (PubSubManager.this) {
          listener = listeners.get(channel);
        }

        if (listener != null) {
          listener.onPubSubMessage(mapper.readValue(message, PubSubMessage.class));
        }
      } catch (IOException e) {
        logger.warn("IOE", e);
      }
    }

    @Override
    public void onPMessage(String s, String s2, String s3) {
      logger.warn("Received PMessage!");
    }

    @Override
    public void onSubscribe(String channel, int count) {
      if (KEEPALIVE_CHANNEL.equals(channel)) {
        synchronized (PubSubManager.this) {
          subscribed = true;
          PubSubManager.this.notifyAll();
        }
      }
    }

    @Override
    public void onUnsubscribe(String s, int i) {}

    @Override
    public void onPUnsubscribe(String s, int i) {}

    @Override
    public void onPSubscribe(String s, int i) {}
  }
}
