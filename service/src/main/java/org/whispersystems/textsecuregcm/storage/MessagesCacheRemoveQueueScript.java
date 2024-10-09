/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import reactor.core.publisher.Mono;

/**
 * Removes a device's queue from the cache. For a non-empty queue, this script must be executed multiple times.
 * <ol>
 *  <li>The first call will return a list of messages to check for {@code sharedMrmKeys}. If a {@code sharedMrmKey} is present, {@link MessagesCacheRemoveRecipientViewFromMrmDataScript} must be called.</li>
 *  <li>Once theses messages have been processed, this script should be called again, confirming that the messages have been processed.</li>
 *  <li>This should be repeated until the script returns an empty list, as the script only returns a page ({@value PAGE_SIZE}) of messages at a time.</li>
 * </ol>
 */
class MessagesCacheRemoveQueueScript {

  private static final int PAGE_SIZE = 100;

  private final ClusterLuaScript removeQueueScript;

  MessagesCacheRemoveQueueScript(FaultTolerantRedisClusterClient redisCluster) throws IOException {
    this.removeQueueScript = ClusterLuaScript.fromResource(redisCluster, "lua/remove_queue.lua",
        ScriptOutputType.MULTI);
  }

  Mono<List<byte[]>> execute(final UUID destinationUuid, final byte destinationDevice,
      final List<String> processedMessageGuids) {

    final List<byte[]> keys = List.of(
        MessagesCache.getMessageQueueKey(destinationUuid, destinationDevice), // queueKey
        MessagesCache.getMessageQueueMetadataKey(destinationUuid, destinationDevice), // queueMetadataKey
        MessagesCache.getQueueIndexKey(destinationUuid, destinationDevice) // queueTotalIndexKey
    );

    final List<byte[]> args = new ArrayList<>();

    args.addFirst(String.valueOf(PAGE_SIZE).getBytes(StandardCharsets.UTF_8)); // limit
    args.addAll(processedMessageGuids.stream().map(guid -> guid.getBytes(StandardCharsets.UTF_8))
        .toList()); // processedMessageGuids

    //noinspection unchecked
    return removeQueueScript.executeBinaryReactive(keys, args)
        .map(result -> (List<byte[]>) result)
        .next();
  }

}
