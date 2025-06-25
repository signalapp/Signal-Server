/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScriptOutputType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class MessagesCacheGetItemsScriptTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  void testCacheGetItemsScript() throws Exception {
    final MessagesCacheInsertScript insertScript = new MessagesCacheInsertScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final UUID destinationUuid = UUID.randomUUID();
    final byte deviceId = 1;
    final String serverGuid = UUID.randomUUID().toString();
    final MessageProtos.Envelope envelope1 = MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(serverGuid)
        .build();

    insertScript.executeAsync(destinationUuid, deviceId, envelope1);

    final MessagesCacheGetItemsScript getItemsScript = new MessagesCacheGetItemsScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final List<byte[]> messageAndScores = getItemsScript.execute(destinationUuid, deviceId, 1, -1)
        .block(Duration.ofSeconds(1));

    assertNotNull(messageAndScores);
    assertEquals(2, messageAndScores.size());
    final MessageProtos.Envelope resultEnvelope =
        EnvelopeUtil.expand(MessageProtos.Envelope.parseFrom(messageAndScores.getFirst()));

    assertEquals(serverGuid, resultEnvelope.getServerGuid());
  }

  @Test
  void testCacheGetItemsInvalidParameter() throws Exception {
    final ClusterLuaScript getItemsScript = ClusterLuaScript.fromResource(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        "lua/get_items.lua", ScriptOutputType.OBJECT);

    final byte[] fakeKey = new byte[]{1};

    final Exception e = assertThrows(RedisCommandExecutionException.class,
        () -> getItemsScript.executeBinaryReactive(List.of(fakeKey, fakeKey),
                List.of("1".getBytes(StandardCharsets.UTF_8)))
            .next()
            .block(Duration.ofSeconds(1)));

    assertEquals("ERR afterMessageId is required", e.getMessage());
  }
}
