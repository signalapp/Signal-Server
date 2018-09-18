/**
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

import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DirectoryReconciliationCache {

  private static final String ACTIVE_WORKER_KEY = "directory_reconciliation_active_worker";
  private static final String LAST_NUMBER_KEY   = "directory_reconciliation_last_number";
  private static final String CACHED_COUNT_KEY  = "directory_reconciliation_cached_count";
  private static final String ACCELERATE_KEY    = "directory_reconciliation_accelerate";

  private static final long CACHED_COUNT_TTL_MS = 21600_000L;
  private static final long LAST_NUMBER_TTL_MS  = 86400_000L;

  private final ReplicatedJedisPool jedisPool;
  private final UnlockOperation     unlockOperation;

  public DirectoryReconciliationCache(ReplicatedJedisPool jedisPool) throws IOException {
    this.jedisPool       = jedisPool;
    this.unlockOperation = new UnlockOperation(jedisPool);
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
    unlockOperation.unlock(ACTIVE_WORKER_KEY, workerId);
    try (Jedis jedis = jedisPool.getWriteResource()) {
      return "OK".equals(jedis.set(ACTIVE_WORKER_KEY, workerId, "NX", "PX", ttlMs));
    }
  }

  public Optional<String> getLastNumber() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      return Optional.fromNullable(jedis.get(LAST_NUMBER_KEY));
    }
  }

  public void setLastNumber(Optional<String> lastNumber) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      if (lastNumber.isPresent()) {
        jedis.psetex(LAST_NUMBER_KEY, LAST_NUMBER_TTL_MS, lastNumber.get());
      } else {
        jedis.del(LAST_NUMBER_KEY);
      }
    }
  }

  public Optional<Long> getCachedAccountCount() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      Optional<String> cachedAccountCount = Optional.fromNullable(jedis.get(CACHED_COUNT_KEY));
      if (!cachedAccountCount.isPresent()) {
        return Optional.absent();
      }

      try {
        return Optional.of(Long.parseUnsignedLong(cachedAccountCount.get()));
      } catch (NumberFormatException ex) {
        return Optional.absent();
      }
    }
  }

  public void setCachedAccountCount(long accountCount) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      jedis.psetex(CACHED_COUNT_KEY, CACHED_COUNT_TTL_MS, Long.toString(accountCount));
    }
  }

  public static class UnlockOperation {

    private final LuaScript luaScript;

    UnlockOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.luaScript = LuaScript.fromResource(jedisPool, "lua/unlock.lua");
    }

    public boolean unlock(String key, String value) {
      List<byte[]> keys = Arrays.asList(key.getBytes());
      List<byte[]> args = Arrays.asList(value.getBytes());

      return ((long) luaScript.execute(keys, args)) > 0;
    }
  }

}
