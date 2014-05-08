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

import org.whispersystems.textsecuregcm.util.Constants;

import java.util.LinkedList;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class StoredMessages {

  private final MetricRegistry metricRegistry     = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Histogram      queueSizeHistogram = metricRegistry.histogram(name(getClass(), "queue_size"));

  private static final String QUEUE_PREFIX = "msgs";

  private final JedisPool jedisPool;

  public StoredMessages(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void insert(long accountId, long deviceId, String message) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      long queueSize = jedis.lpush(getKey(accountId, deviceId), message);
      queueSizeHistogram.update(queueSize);

      if (queueSize > 1000) {
        jedis.ltrim(getKey(accountId, deviceId), 0, 999);
      }
    } finally {
      if (jedis != null)
        jedisPool.returnResource(jedis);
    }
  }

  public List<String> getMessagesForDevice(long accountId, long deviceId) {
    List<String> messages = new LinkedList<>();
    Jedis        jedis    = null;

    try {
      jedis = jedisPool.getResource();
      String message;

      while ((message = jedis.rpop(QUEUE_PREFIX + accountId + ":" + deviceId)) != null) {
        messages.add(message);
      }

      return messages;
    } finally {
      if (jedis != null)
        jedisPool.returnResource(jedis);
    }
  }

  private String getKey(long accountId, long deviceId) {
    return QUEUE_PREFIX + ":" + accountId + ":" + deviceId;
  }

}