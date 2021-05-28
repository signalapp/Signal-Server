/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class DeletedAccountsTableCrawlerCache {

  private static final String ACTIVE_WORKER_KEY = "deleted_accounts_crawler_cache_active_worker";

  private final FaultTolerantRedisCluster cacheCluster;
  private final ClusterLuaScript unlockClusterScript;

  public DeletedAccountsTableCrawlerCache(final FaultTolerantRedisCluster cacheCluster) throws IOException {
    this.cacheCluster = cacheCluster;
    this.unlockClusterScript = ClusterLuaScript.fromResource(cacheCluster, "lua/table_crawler/unlock.lua", ScriptOutputType.INTEGER);
  }

  public boolean claimActiveWork(String workerId, Duration ttl) {
    return "OK".equals(cacheCluster.withCluster(connection -> connection.sync().set(ACTIVE_WORKER_KEY, workerId, SetArgs.Builder.nx().px(ttl.toMillis()))));
  }

  public void releaseActiveWork(String workerId) {
    unlockClusterScript.execute(List.of(ACTIVE_WORKER_KEY), List.of(workerId));
  }
}
