/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.Util;

/**
 * Inserts the shared multi-recipient message payload into the cache. The list of recipients and views will be set as
 * fields in the hash.
 *
 * @see SealedSenderMultiRecipientMessage#serializedRecipientView(SealedSenderMultiRecipientMessage.Recipient)
 */
class MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript {

  private final ClusterLuaScript script;
  private final ScheduledExecutorService retryExecutor;

  static final String ERROR_KEY_EXISTS = "ERR key exists";

  MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(FaultTolerantRedisClusterClient redisCluster,
      final ScheduledExecutorService retryExecutor) throws IOException {

    this.script = ClusterLuaScript.fromResource(redisCluster, "lua/insert_shared_multirecipient_message_data.lua",
        ScriptOutputType.INTEGER);

    this.retryExecutor = retryExecutor;
  }

  CompletionStage<Void> executeAsync(final byte[] sharedMrmKey,
      final SealedSenderMultiRecipientMessage message,
      final Set<SealedSenderMultiRecipientMessage.Recipient> resolvedRecipients) {

    final List<byte[]> keys = List.of(
        sharedMrmKey // sharedMrmKey
    );

    final int deviceCount = message.getRecipients().values().stream()
        .filter(resolvedRecipients::contains)
        .mapToInt(recipient -> recipient.getDevices().length)
        .sum();

    // Pre-allocate capacity for the most fields we expect -- the shared data field plus one for each device
    final List<byte[]> args = new ArrayList<>(1 + deviceCount);
    args.add(message.serialized());

    message.getRecipients().forEach((serviceId, recipient) -> {
      if (!resolvedRecipients.contains(recipient)) {
        return;
      }

      for (final byte deviceId : recipient.getDevices()) {
        args.add(MessagesCache.getSharedMrmViewKey(serviceId, deviceId));
        args.add(message.serializedRecipientView(recipient));
      }
    });

    return ResilienceUtil.getGeneralRedisRetry(MessagesCache.RETRY_NAME)
        .executeCompletionStage(retryExecutor, () -> script.executeBinaryAsync(keys, args))
        .thenRun(Util.NOOP);
  }
}
