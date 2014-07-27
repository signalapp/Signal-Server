/**
 * Copyright (C) 2014 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PendingMessage;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class StoredMessages {

  private static final Logger logger = LoggerFactory.getLogger(StoredMessages.class);

  private final MetricRegistry metricRegistry     = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Histogram      queueSizeHistogram = metricRegistry.histogram(name(getClass(), "queue_size"));


  private static final ObjectMapper mapper = SystemMapper.getMapper();
  private static final String QUEUE_PREFIX = "msgs";

  private final JedisPool jedisPool;

  public StoredMessages(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void clear(WebsocketAddress address) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();
      jedis.del(getKey(address));
    } finally {
      if (jedis != null)
        jedisPool.returnResource(jedis);
    }
  }

  public void insert(WebsocketAddress address, PendingMessage message) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      String serializedMessage = mapper.writeValueAsString(message);
      long   queueSize         = jedis.lpush(getKey(address), serializedMessage);
      queueSizeHistogram.update(queueSize);

      if (queueSize > 1000) {
        jedis.ltrim(getKey(address), 0, 999);
      }

    } catch (JsonProcessingException e) {
      logger.warn("StoredMessages", "Unable to store correctly", e);
    } finally {
      if (jedis != null)
        jedisPool.returnResource(jedis);
    }
  }

  public List<PendingMessage> getMessagesForDevice(WebsocketAddress address) {
    List<PendingMessage> messages = new LinkedList<>();
    Jedis                jedis    = null;

    try {
      jedis = jedisPool.getResource();
      String message;

      while ((message = jedis.rpop(getKey(address))) != null) {
        try {
          messages.add(mapper.readValue(message, PendingMessage.class));
        } catch (IOException e) {
          logger.warn("StoredMessages", "Not a valid PendingMessage", e);
        }
      }

      return messages;
    } finally {
      if (jedis != null)
        jedisPool.returnResource(jedis);
    }
  }

  private String getKey(WebsocketAddress address) {
    return QUEUE_PREFIX + ":" + address.serialize();
  }

}