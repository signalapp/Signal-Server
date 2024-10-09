/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

/**
 * Inserts an envelope into the message queue for a destination device.
 */
class MessagesCacheInsertScript {

  private final ClusterLuaScript insertScript;

  MessagesCacheInsertScript(FaultTolerantRedisClusterClient redisCluster) throws IOException {
    this.insertScript = ClusterLuaScript.fromResource(redisCluster, "lua/insert_item.lua", ScriptOutputType.INTEGER);
  }

  long execute(final UUID destinationUuid, final byte destinationDevice, final MessageProtos.Envelope envelope) {
    assert envelope.hasServerGuid();
    assert envelope.hasServerTimestamp();

    final List<byte[]> keys = List.of(
        MessagesCache.getMessageQueueKey(destinationUuid, destinationDevice), // queueKey
        MessagesCache.getMessageQueueMetadataKey(destinationUuid, destinationDevice), // queueMetadataKey
        MessagesCache.getQueueIndexKey(destinationUuid, destinationDevice) // queueTotalIndexKey
    );

    final List<byte[]> args = new ArrayList<>(Arrays.asList(
        envelope.toByteArray(), // message
        String.valueOf(envelope.getServerTimestamp()).getBytes(StandardCharsets.UTF_8), // currentTime
        envelope.getServerGuid().getBytes(StandardCharsets.UTF_8) // guid
    ));

    return (long) insertScript.executeBinary(keys, args);
  }
}
