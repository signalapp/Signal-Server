/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class MessagesCacheRemoveByGuidScriptTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  void testCacheRemoveByGuid() throws Exception {
    final MessagesCacheInsertScript insertScript = new MessagesCacheInsertScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final UUID destinationUuid = UUID.randomUUID();
    final byte deviceId = 1;
    final UUID serverGuid = UUID.randomUUID();
    final MessageProtos.Envelope envelope1 = MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(serverGuid.toString())
        .build();

    insertScript.executeAsync(destinationUuid, deviceId, envelope1);

    final MessagesCacheRemoveByGuidScript removeByGuidScript = new MessagesCacheRemoveByGuidScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final List<byte[]> removedMessages = removeByGuidScript.execute(destinationUuid, deviceId,
        List.of(serverGuid)).get(1, TimeUnit.SECONDS);

    assertEquals(1, removedMessages.size());

    final MessageProtos.Envelope resultMessage = MessageProtos.Envelope.parseFrom(removedMessages.getFirst());

    assertEquals(serverGuid, UUIDUtil.fromByteString(resultMessage.getServerGuidBinary()));
  }
}
