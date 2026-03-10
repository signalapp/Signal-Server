/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import java.io.IOException;
import java.util.List;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

class MessagesCacheReleaseNodeClaimScript {

  private final ClusterLuaScript releaseNodeClaimScript;

  MessagesCacheReleaseNodeClaimScript(final FaultTolerantRedisClusterClient redisCluster) throws IOException {
    this.releaseNodeClaimScript =
        ClusterLuaScript.fromResource(redisCluster, "lua/release_node_claim.lua", ScriptOutputType.STATUS);
  }

  void execute(final RedisClusterNode node, final String persisterId) {
    final List<String> keys = List.of(MessagesCache.getPersisterNodeClaimKey(node));
    final List<String> arguments = List.of(persisterId);

    ResilienceUtil.getGeneralRedisRetry(getClass().getSimpleName())
        .executeRunnable(() -> releaseNodeClaimScript.execute(keys, arguments));
  }
}
