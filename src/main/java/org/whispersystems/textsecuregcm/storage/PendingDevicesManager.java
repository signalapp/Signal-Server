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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class PendingDevicesManager {

  private final Logger logger = LoggerFactory.getLogger(PendingDevicesManager.class);

  private static final String CACHE_PREFIX = "pending_devices2::";

  private final PendingDevices  pendingDevices;
  private final JedisPool       cacheClient;
  private final ObjectMapper    mapper;

  public PendingDevicesManager(PendingDevices pendingDevices,
                               JedisPool      cacheClient)
  {
    this.pendingDevices = pendingDevices;
    this.cacheClient    = cacheClient;
    this.mapper         = SystemMapper.getMapper();
  }

  public void store(String number, StoredVerificationCode code) {
    memcacheSet(number, code);
    pendingDevices.insert(number, code.getCode(), code.getTimestamp());
  }

  public void remove(String number) {
    memcacheDelete(number);
    pendingDevices.remove(number);
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    Optional<StoredVerificationCode> code = memcacheGet(number);

    if (!code.isPresent()) {
      code = Optional.fromNullable(pendingDevices.getCodeForNumber(number));

      if (code.isPresent()) {
        memcacheSet(number, code.get());
      }
    }

    return code;
  }

  private void memcacheSet(String number, StoredVerificationCode code) {
    try (Jedis jedis = cacheClient.getResource()) {
      jedis.set(CACHE_PREFIX + number, mapper.writeValueAsString(code));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private Optional<StoredVerificationCode> memcacheGet(String number) {
    try (Jedis jedis = cacheClient.getResource()) {
      String json = jedis.get(CACHE_PREFIX + number);

      if (json == null) return Optional.absent();
      else              return Optional.of(mapper.readValue(json, StoredVerificationCode.class));
    } catch (IOException e) {
      logger.warn("Could not parse pending device stored verification json");
      return Optional.absent();
    }
  }

  private void memcacheDelete(String number) {
    try (Jedis jedis = cacheClient.getResource()) {
      jedis.del(CACHE_PREFIX + number);
    }
  }

}
