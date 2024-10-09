/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

/**
 * Removes a list of message GUIDs from the queue of a destination device.
 */
class MessagesCacheRemoveByGuidScript {

  private final ClusterLuaScript removeByGuidScript;

  MessagesCacheRemoveByGuidScript(final FaultTolerantRedisClusterClient redisCluster) throws IOException {
    this.removeByGuidScript = ClusterLuaScript.fromResource(redisCluster, "lua/remove_item_by_guid.lua",
        ScriptOutputType.OBJECT);
  }

  CompletableFuture<List<byte[]>> execute(final UUID destinationUuid, final byte destinationDevice,
      final List<UUID> messageGuids) {

    final List<byte[]> keys = List.of(
        MessagesCache.getMessageQueueKey(destinationUuid, destinationDevice), // queueKey
        MessagesCache.getMessageQueueMetadataKey(destinationUuid, destinationDevice), // queueMetadataKey
        MessagesCache.getQueueIndexKey(destinationUuid, destinationDevice) // queueTotalIndexKey
    );
    final List<byte[]> args = messageGuids.stream().map(guid -> guid.toString().getBytes(StandardCharsets.UTF_8))
        .toList();

    //noinspection unchecked
    return removeByGuidScript.executeBinaryAsync(keys, args)
        .thenApply(result -> (List<byte[]>) result);
  }

}
