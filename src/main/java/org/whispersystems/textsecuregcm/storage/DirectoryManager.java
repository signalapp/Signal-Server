/**
 * Copyright (C) 2013 Open WhisperSystems
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.util.IterablePair;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class DirectoryManager {

  private final Logger logger = LoggerFactory.getLogger(DirectoryManager.class);

  private static final byte[] DIRECTORY_KEY = {'d', 'i', 'r', 'e', 'c', 't', 'o', 'r', 'y'};

  private final ObjectMapper objectMapper;
  private final JedisPool redisPool;

  public DirectoryManager(JedisPool redisPool) {
    this.redisPool    = redisPool;
    this.objectMapper = new ObjectMapper();
    this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public void remove(String number) {
    remove(Util.getContactToken(number));
  }

  public void remove(BatchOperationHandle handle, String number) {
    remove(handle, Util.getContactToken(number));
  }

  public void remove(byte[] token) {
    try (Jedis jedis = redisPool.getResource()) {
      jedis.hdel(DIRECTORY_KEY, token);
    }
  }

  public void remove(BatchOperationHandle handle, byte[] token) {
    Pipeline pipeline = handle.pipeline;
    pipeline.hdel(DIRECTORY_KEY, token);
  }

  public void add(ClientContact contact) {
    TokenValue tokenValue = new TokenValue(contact.getRelay(), contact.isVoice(), contact.isVideo());

    try (Jedis jedis = redisPool.getResource()) {
      jedis.hset(DIRECTORY_KEY, contact.getToken(), objectMapper.writeValueAsBytes(tokenValue));
    } catch (JsonProcessingException e) {
      logger.warn("JSON Serialization", e);
    }
  }

  public void add(BatchOperationHandle handle, ClientContact contact) {
    try {
      Pipeline   pipeline   = handle.pipeline;
      TokenValue tokenValue = new TokenValue(contact.getRelay(), contact.isVoice(), contact.isVideo());

      pipeline.hset(DIRECTORY_KEY, contact.getToken(), objectMapper.writeValueAsBytes(tokenValue));
    } catch (JsonProcessingException e) {
      logger.warn("JSON Serialization", e);
    }
  }

  public PendingClientContact get(BatchOperationHandle handle, byte[] token) {
    Pipeline pipeline = handle.pipeline;
    return new PendingClientContact(objectMapper, token, pipeline.hget(DIRECTORY_KEY, token));
  }

  public Optional<ClientContact> get(byte[] token) {
    try (Jedis jedis = redisPool.getResource()) {
      byte[] result = jedis.hget(DIRECTORY_KEY, token);

      if (result == null) {
        return Optional.absent();
      }

      TokenValue tokenValue = objectMapper.readValue(result, TokenValue.class);
      return Optional.of(new ClientContact(token, tokenValue.relay, tokenValue.voice, tokenValue.video));
    } catch (IOException e) {
      logger.warn("JSON Error", e);
      return Optional.absent();
    }
  }

  public List<ClientContact> get(List<byte[]> tokens) {
    try (Jedis jedis = redisPool.getResource()) {
      Pipeline               pipeline = jedis.pipelined();
      List<Response<byte[]>> futures  = new LinkedList<>();
      List<ClientContact>    results  = new LinkedList<>();

      try {
        for (byte[] token : tokens) {
          futures.add(pipeline.hget(DIRECTORY_KEY, token));
        }
      } finally {
        pipeline.sync();
      }

      IterablePair<byte[], Response<byte[]>> lists = new IterablePair<>(tokens, futures);

      for (Pair<byte[], Response<byte[]>> pair : lists) {
        try {
          if (pair.second().get() != null) {
            TokenValue    tokenValue    = objectMapper.readValue(pair.second().get(), TokenValue.class);
            ClientContact clientContact = new ClientContact(pair.first(), tokenValue.relay, tokenValue.voice, tokenValue.video);

            results.add(clientContact);
          }
        } catch (IOException e) {
          logger.warn("Deserialization Problem: ", e);
        }
      }

      return results;
    }
  }

  public BatchOperationHandle startBatchOperation() {
    Jedis jedis = redisPool.getResource();
    return new BatchOperationHandle(jedis, jedis.pipelined());
  }

  public void stopBatchOperation(BatchOperationHandle handle) {
    Pipeline pipeline = handle.pipeline;
    Jedis    jedis    = handle.jedis;

    pipeline.sync();
    redisPool.returnResource(jedis);
  }

  public static class BatchOperationHandle {

    public final Pipeline pipeline;
    public final Jedis    jedis;

    public BatchOperationHandle(Jedis jedis, Pipeline pipeline) {
      this.pipeline = pipeline;
      this.jedis    = jedis;
    }
  }

  private static class TokenValue {

    @JsonProperty(value = "r")
    private String  relay;

    @JsonProperty(value = "v")
    private boolean voice;

    @JsonProperty(value = "w")
    private boolean video;

    public TokenValue() {}

    public TokenValue(String relay, boolean voice, boolean video) {
      this.relay = relay;
      this.voice = voice;
      this.video = video;
    }
  }

  public static class PendingClientContact {
    private final ObjectMapper     objectMapper;
    private final byte[]           token;
    private final Response<byte[]> response;

    PendingClientContact(ObjectMapper objectMapper, byte[] token, Response<byte[]> response) {
      this.objectMapper = objectMapper;
      this.token        = token;
      this.response     = response;
    }

    public Optional<ClientContact> get() throws IOException {
      byte[] result = response.get();

      if (result == null) {
        return Optional.absent();
      }

      TokenValue tokenValue = objectMapper.readValue(result, TokenValue.class);
      return Optional.of(new ClientContact(token, tokenValue.relay, tokenValue.voice, tokenValue.video));
    }

  }
}
