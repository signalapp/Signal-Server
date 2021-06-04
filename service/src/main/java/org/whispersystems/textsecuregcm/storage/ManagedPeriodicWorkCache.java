/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

public class ManagedPeriodicWorkCache {

  private final String activeWorkerKey;

  private final FaultTolerantRedisCluster cacheCluster;
  private final ClusterLuaScript unlockClusterScript;

  public ManagedPeriodicWorkCache(final String activeWorkerKey, final FaultTolerantRedisCluster cacheCluster) throws IOException {
    this.activeWorkerKey = activeWorkerKey;
    this.cacheCluster = cacheCluster;
    this.unlockClusterScript = ClusterLuaScript.fromResource(cacheCluster, "lua/periodic_worker/unlock.lua", ScriptOutputType.INTEGER);
  }

  public boolean claimActiveWork(String workerId, Duration ttl) {
    return "OK".equals(cacheCluster.withCluster(connection -> connection.sync().set(activeWorkerKey, workerId, SetArgs.Builder.nx().px(ttl.toMillis()))));
  }

  public void releaseActiveWork(String workerId) {
    unlockClusterScript.execute(List.of(activeWorkerKey), List.of(workerId));
  }
}
