/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

/**
 * Inserts the shared multi-recipient message payload into the cache. The list of recipients and views will be set as
 * fields in the hash.
 *
 * @see SealedSenderMultiRecipientMessage#serializedRecipientView(SealedSenderMultiRecipientMessage.Recipient)
 */
class MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript {

  private final ClusterLuaScript script;

  static final String ERROR_KEY_EXISTS = "ERR key exists";

  MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(FaultTolerantRedisCluster redisCluster)
      throws IOException {
    this.script = ClusterLuaScript.fromResource(redisCluster, "lua/insert_shared_multirecipient_message_data.lua",
        ScriptOutputType.INTEGER);
  }

  void execute(final byte[] sharedMrmKey, final SealedSenderMultiRecipientMessage message) {
    final List<byte[]> keys = List.of(
        sharedMrmKey // sharedMrmKey
    );

    // Pre-allocate capacity for the most fields we expect -- 6 devices per recipient, plus the data field.
    final List<byte[]> args = new ArrayList<>(message.getRecipients().size() * 6 + 1);
    args.add(message.serialized());

    message.getRecipients().forEach((serviceId, recipient) -> {
      for (byte device : recipient.getDevices()) {
        args.add(MessagesCache.getSharedMrmViewKey(serviceId, device));
        args.add(message.serializedRecipientView(recipient));
      }
    });

    script.executeBinary(keys, args);
  }
}
