/*
 * Copyright (C) 2018 Open WhisperSystems
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import redis.clients.jedis.Jedis;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class AccountDatabaseCrawlerCache {

  private static final String ACTIVE_WORKER_KEY = "account_database_crawler_cache_active_worker";
  private static final String LAST_UUID_KEY     = "account_database_crawler_cache_last_uuid";
  private static final String ACCELERATE_KEY    = "account_database_crawler_cache_accelerate";

  private static final long LAST_NUMBER_TTL_MS  = 86400_000L;

  private final ReplicatedJedisPool jedisPool;
  private final LuaScript           luaScript;

  public AccountDatabaseCrawlerCache(ReplicatedJedisPool jedisPool) throws IOException {
    this.jedisPool = jedisPool;
    this.luaScript = LuaScript.fromResource(jedisPool, "lua/account_database_crawler/unlock.lua");
  }

  public void clearAccelerate() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      jedis.del(ACCELERATE_KEY);
    }
  }

  public boolean isAccelerated() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      return "1".equals(jedis.get(ACCELERATE_KEY));
    }
  }

  public boolean claimActiveWork(String workerId, long ttlMs) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      return "OK".equals(jedis.set(ACTIVE_WORKER_KEY, workerId, "NX", "PX", ttlMs));
    }
  }

  public void releaseActiveWork(String workerId) {
    List<byte[]> keys = Arrays.asList(ACTIVE_WORKER_KEY.getBytes());
    List<byte[]> args = Arrays.asList(workerId.getBytes());
    luaScript.execute(keys, args);
  }

  public Optional<UUID> getLastUuid() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      String lastUuidString = jedis.get(LAST_UUID_KEY);

      if (lastUuidString == null) return Optional.empty();
      else                        return Optional.of(UUID.fromString(lastUuidString));
    }
  }

  public void setLastUuid(Optional<UUID> lastUuid) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      if (lastUuid.isPresent()) {
        jedis.psetex(LAST_UUID_KEY, LAST_NUMBER_TTL_MS, lastUuid.get().toString());
      } else {
        jedis.del(LAST_UUID_KEY);
      }
    }
  }

}
