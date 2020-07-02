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

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.experiment.Experiment;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
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

  private static final Logger log = LoggerFactory.getLogger(AccountDatabaseCrawlerCache.class);

  private final ReplicatedJedisPool       jedisPool;
  private final FaultTolerantRedisCluster cacheCluster;
  private final LuaScript                 unlockScript;
  private final ClusterLuaScript          unlockClusterScript;
  private final Experiment                isAcceleratedExperiment = new Experiment("RedisCluster", "AccountDatabaseCrawlerCache", "isAccelerated");
  private final Experiment                getLastUuidExperiment   = new Experiment("RedisCluster", "AccountDatabaseCrawlerCache", "getLastUuid");

  public AccountDatabaseCrawlerCache(ReplicatedJedisPool jedisPool, FaultTolerantRedisCluster cacheCluster) throws IOException {
    this.jedisPool           = jedisPool;
    this.cacheCluster        = cacheCluster;
    this.unlockScript        = LuaScript.fromResource(jedisPool, "lua/account_database_crawler/unlock.lua");
    this.unlockClusterScript = ClusterLuaScript.fromResource(cacheCluster, "lua/account_database_crawler/unlock.lua", ScriptOutputType.INTEGER);
  }

  public void clearAccelerate() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      jedis.del(ACCELERATE_KEY);
      cacheCluster.useWriteCluster(connection -> connection.sync().del(ACCELERATE_KEY));
    }
  }

  public boolean isAccelerated() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      final String accelerated = jedis.get(ACCELERATE_KEY);
      isAcceleratedExperiment.compareSupplierResult(accelerated, () -> cacheCluster.withReadCluster(connection -> connection.sync().get(ACCELERATE_KEY)));

      return "1".equals(accelerated);
    }
  }

  public boolean claimActiveWork(String workerId, long ttlMs) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      final boolean claimed = "OK".equals(jedis.set(ACTIVE_WORKER_KEY, workerId, "NX", "PX", ttlMs));

      if (claimed) {
        // TODO Restore the NX flag when making the cluster the primary data store
        cacheCluster.useWriteCluster(connection -> connection.sync().set(ACTIVE_WORKER_KEY, workerId, SetArgs.Builder.px(ttlMs)));
      }

      return claimed;
    }
  }

  public void releaseActiveWork(String workerId) {
    List<byte[]> keys = Arrays.asList(ACTIVE_WORKER_KEY.getBytes());
    List<byte[]> args = Arrays.asList(workerId.getBytes());
    unlockScript.execute(keys, args);

    /* try {
      unlockClusterScript.execute(List.of(ACTIVE_WORKER_KEY), List.of(workerId));
    } catch (Exception e) {
      log.warn("Failed to execute clustered unlock script", e);
    } */
  }

  public Optional<UUID> getLastUuid() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      String lastUuidString = jedis.get(LAST_UUID_KEY);
      getLastUuidExperiment.compareSupplierResult(lastUuidString, () -> cacheCluster.withWriteCluster(connection -> connection.sync().get(LAST_UUID_KEY)));

      if (lastUuidString == null) return Optional.empty();
      else                        return Optional.of(UUID.fromString(lastUuidString));
    }
  }

  public void setLastUuid(Optional<UUID> lastUuid) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      if (lastUuid.isPresent()) {
        jedis.psetex(LAST_UUID_KEY, LAST_NUMBER_TTL_MS, lastUuid.get().toString());
        cacheCluster.useWriteCluster(connection -> connection.sync().psetex(LAST_UUID_KEY, LAST_NUMBER_TTL_MS, lastUuid.get().toString()));
      } else {
        jedis.del(LAST_UUID_KEY);
        cacheCluster.useWriteCluster(connection -> connection.sync().del(LAST_UUID_KEY));
      }
    }
  }

}
