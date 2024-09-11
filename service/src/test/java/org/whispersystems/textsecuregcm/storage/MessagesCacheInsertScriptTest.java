/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class MessagesCacheInsertScriptTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  void testCacheInsertScript() throws Exception {
    final MessagesCacheInsertScript insertScript = new MessagesCacheInsertScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final UUID destinationUuid = UUID.randomUUID();
    final byte deviceId = 1;
    final MessageProtos.Envelope envelope1 = MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(UUID.randomUUID().toString())
        .build();

    assertEquals(1, insertScript.execute(destinationUuid, deviceId, envelope1));

    final MessageProtos.Envelope envelope2 = MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(UUID.randomUUID().toString())
        .build();
    assertEquals(2, insertScript.execute(destinationUuid, deviceId, envelope2));

    assertEquals(1, insertScript.execute(destinationUuid, deviceId, envelope1),
        "Repeated with same guid should have same message ID");
  }
}
