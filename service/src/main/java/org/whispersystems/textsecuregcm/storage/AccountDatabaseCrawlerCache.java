/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class AccountDatabaseCrawlerCache {

  private static final String ACTIVE_WORKER_KEY = "account_database_crawler_cache_active_worker";
  private static final String LAST_UUID_KEY     = "account_database_crawler_cache_last_uuid";
  private static final String ACCELERATE_KEY    = "account_database_crawler_cache_accelerate";

  private static final long LAST_NUMBER_TTL_MS  = 86400_000L;

  private final FaultTolerantRedisCluster cacheCluster;
  private final ClusterLuaScript          unlockClusterScript;

  public AccountDatabaseCrawlerCache(FaultTolerantRedisCluster cacheCluster) throws IOException {
    this.cacheCluster        = cacheCluster;
    this.unlockClusterScript = ClusterLuaScript.fromResource(cacheCluster, "lua/account_database_crawler/unlock.lua", ScriptOutputType.INTEGER);
  }

  public void setAccelerated(final boolean accelerated) {
    if (accelerated) {
      cacheCluster.useCluster(connection -> connection.sync().set(ACCELERATE_KEY, "1"));
    } else {
      cacheCluster.useCluster(connection -> connection.sync().del(ACCELERATE_KEY));
    }
  }

  public boolean isAccelerated() {
    return "1".equals(cacheCluster.withCluster(connection -> connection.sync().get(ACCELERATE_KEY)));
  }

  public boolean claimActiveWork(String workerId, long ttlMs) {
    return "OK".equals(cacheCluster.withCluster(connection -> connection.sync().set(ACTIVE_WORKER_KEY, workerId, SetArgs.Builder.nx().px(ttlMs))));
  }

  public void releaseActiveWork(String workerId) {
    unlockClusterScript.execute(List.of(ACTIVE_WORKER_KEY), List.of(workerId));
  }

  public Optional<UUID> getLastUuid() {
    final String lastUuidString = cacheCluster.withCluster(connection -> connection.sync().get(LAST_UUID_KEY));

    if (lastUuidString == null) return Optional.empty();
    else                        return Optional.of(UUID.fromString(lastUuidString));
  }

  public void setLastUuid(Optional<UUID> lastUuid) {
    if (lastUuid.isPresent()) {
      cacheCluster.useCluster(connection -> connection.sync().psetex(LAST_UUID_KEY, LAST_NUMBER_TTL_MS, lastUuid.get().toString()));
    } else {
      cacheCluster.useCluster(connection -> connection.sync().del(LAST_UUID_KEY));
    }
  }

}
