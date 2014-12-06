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
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.LinkedList;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;
import static org.whispersystems.textsecuregcm.storage.StoredMessageProtos.StoredMessage;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class StoredMessages {

  private static final Logger logger = LoggerFactory.getLogger(StoredMessages.class);

  private final MetricRegistry metricRegistry     = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Histogram      queueSizeHistogram = metricRegistry.histogram(name(getClass(), "queue_size"));

  private static final String QUEUE_PREFIX = "msgs";

  private final JedisPool jedisPool;

  public StoredMessages(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void clear(WebsocketAddress address) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.del(getKey(address));
    }
  }

  public void insert(WebsocketAddress address, OutgoingMessageSignal message) {
    try (Jedis jedis = jedisPool.getResource()) {
      StoredMessage storedMessage = StoredMessage.newBuilder()
                                                 .setType(StoredMessage.Type.MESSAGE)
                                                 .setContent(message.toByteString())
                                                 .build();

      long queueSize = jedis.lpush(getKey(address), storedMessage.toByteArray());
      queueSizeHistogram.update(queueSize);

      if (queueSize > 1000) {
        jedis.ltrim(getKey(address), 0, 999);
      }
    }
  }

  public List<OutgoingMessageSignal> getMessagesForDevice(WebsocketAddress address) {
    List<OutgoingMessageSignal> messages = new LinkedList<>();

    try (Jedis jedis = jedisPool.getResource()) {
      byte[] message;

      while ((message = jedis.rpop(getKey(address))) != null) {
        try {
          StoredMessage storedMessage = StoredMessage.parseFrom(message);

          if (storedMessage.getType().getNumber() == StoredMessage.Type.MESSAGE_VALUE) {
            messages.add(OutgoingMessageSignal.parseFrom(storedMessage.getContent()));
          } else {
            logger.warn("Unkown stored message type: " + storedMessage.getType().getNumber());
          }

        } catch (InvalidProtocolBufferException e) {
          logger.warn("Error parsing protobuf", e);
        }
      }

      return messages;
    }
  }

  private byte[] getKey(WebsocketAddress address) {
    return (QUEUE_PREFIX + ":" + address.serialize()).getBytes();
  }

}