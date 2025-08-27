/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.github.resilience4j.reactor.retry.RetryOperator;
import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import reactor.core.publisher.Mono;

/**
 * Retrieves a list of messages and their corresponding queue-local IDs for the device. To support streaming processing,
 * the last queue-local message ID from a previous call may be used as the {@code afterMessageId}.
 */
class MessagesCacheGetItemsScript {

  private final ClusterLuaScript getItemsScript;

  MessagesCacheGetItemsScript(FaultTolerantRedisClusterClient redisCluster) throws IOException {
    this.getItemsScript = ClusterLuaScript.fromResource(redisCluster, "lua/get_items.lua", ScriptOutputType.OBJECT);
  }

  Mono<List<byte[]>> execute(final UUID destinationUuid, final byte destinationDevice,
      int limit, long afterMessageId) {
    final List<byte[]> keys = List.of(
        MessagesCache.getMessageQueueKey(destinationUuid, destinationDevice), // queueKey
        MessagesCache.getPersistInProgressKey(destinationUuid, destinationDevice) // queueLockKey
    );
    final List<byte[]> args = List.of(
        String.valueOf(limit).getBytes(StandardCharsets.UTF_8), // limit
        String.valueOf(afterMessageId).getBytes(StandardCharsets.UTF_8) // afterMessageId
    );
    //noinspection unchecked
    return getItemsScript.executeBinaryReactive(keys, args)
        .transformDeferred(RetryOperator.of(ResilienceUtil.getGeneralRedisRetry(MessagesCache.RETRY_NAME)))
        .map(result -> (List<byte[]>) result)
        .next();
  }

}
