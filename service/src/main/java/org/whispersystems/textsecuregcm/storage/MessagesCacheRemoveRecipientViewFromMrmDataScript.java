/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import reactor.core.publisher.Mono;

/**
 * Removes the given destination device from the given {@code sharedMrmKeys}. If there are no devices remaining in the
 * hash as a result, the shared payload is deleted.
 * <p>
 * NOTE: Callers are responsible for ensuring that all keys are in the same slot.
 */
class MessagesCacheRemoveRecipientViewFromMrmDataScript {

  private final ClusterLuaScript removeRecipientViewFromMrmDataScript;

  MessagesCacheRemoveRecipientViewFromMrmDataScript(final FaultTolerantRedisClusterClient redisCluster) throws IOException {
    this.removeRecipientViewFromMrmDataScript = ClusterLuaScript.fromResource(redisCluster,
        "lua/remove_recipient_view_from_mrm_data.lua", ScriptOutputType.INTEGER);
  }

  Mono<Long> execute(final Collection<byte[]> keysCollection, final ServiceIdentifier serviceIdentifier,
      final byte deviceId) {

    final List<byte[]> keys = keysCollection instanceof List<byte[]>
        ? (List<byte[]>) keysCollection
        : new ArrayList<>(keysCollection);

    return removeRecipientViewFromMrmDataScript.executeBinaryReactive(keys,
            List.of(MessagesCache.getSharedMrmViewKey(serviceIdentifier, deviceId)))
        .map(o -> (long) o)
        .next();
  }
}
