/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import org.whispersystems.textsecuregcm.push.ClientEvent;
import org.whispersystems.textsecuregcm.push.MessagesPersistedEvent;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Unlocks a message queue for persistence/message retrieval.
 */
class MessagesCacheUnlockQueueScript {

  private final ClusterLuaScript unlockQueueScript;

  private final List<byte[]> MESSAGES_PERSISTED_EVENT_ARGS = List.of(ClientEvent.newBuilder()
      .setMessagesPersisted(MessagesPersistedEvent.getDefaultInstance())
      .build()
      .toByteArray()); // eventPayload

  MessagesCacheUnlockQueueScript(final FaultTolerantRedisClusterClient redisCluster) throws IOException {
    this.unlockQueueScript =
        ClusterLuaScript.fromResource(redisCluster, "lua/unlock_queue.lua", ScriptOutputType.STATUS);
  }

  void execute(final UUID accountIdentifier, final byte deviceId) {
    final List<byte[]> keys = List.of(
        MessagesCache.getPersistInProgressKey(accountIdentifier, deviceId), // persistInProgressKey
        RedisMessageAvailabilityManager.getClientEventChannel(accountIdentifier, deviceId) // eventChannelKey
    );

    ResilienceUtil.getGeneralRedisRetry(MessagesCache.RETRY_NAME)
        .executeRunnable(() -> unlockQueueScript.executeBinary(keys, MESSAGES_PERSISTED_EVENT_ARGS));
  }
}
