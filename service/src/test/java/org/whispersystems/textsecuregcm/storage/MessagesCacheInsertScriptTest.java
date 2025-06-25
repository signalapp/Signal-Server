/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.WebSocketConnectionEventManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubClusterConnection;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class MessagesCacheInsertScriptTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  void testCacheInsertScript() throws Exception {
    final MessagesCacheInsertScript insertScript =
        new MessagesCacheInsertScript(REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final UUID destinationUuid = UUID.randomUUID();
    final byte deviceId = 1;
    final MessageProtos.Envelope envelope1 = MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(UUID.randomUUID().toString())
        .build();

    insertScript.executeAsync(destinationUuid, deviceId, envelope1);

    assertEquals(List.of(EnvelopeUtil.compress(envelope1)), getStoredMessages(destinationUuid, deviceId));

    final MessageProtos.Envelope envelope2 = MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(UUID.randomUUID().toString())
        .build();

    insertScript.executeAsync(destinationUuid, deviceId, envelope2);

    assertEquals(List.of(EnvelopeUtil.compress(envelope1), EnvelopeUtil.compress(envelope2)),
        getStoredMessages(destinationUuid, deviceId));

    insertScript.executeAsync(destinationUuid, deviceId, envelope1);

    assertEquals(List.of(EnvelopeUtil.compress(envelope1), EnvelopeUtil.compress(envelope2)),
        getStoredMessages(destinationUuid, deviceId),
        "Messages with same GUID should be deduplicated");
  }

  private List<MessageProtos.Envelope> getStoredMessages(final UUID destinationUuid, final byte deviceId) throws IOException {
    final MessagesCacheGetItemsScript getItemsScript =
        new MessagesCacheGetItemsScript(REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final List<byte[]> queueItems = getItemsScript.execute(destinationUuid, deviceId, 1024, 0)
        .blockOptional()
        .orElseGet(Collections::emptyList);

    final List<MessageProtos.Envelope> messages = new ArrayList<>(queueItems.size() / 2);

    for (int i = 0; i < queueItems.size(); i += 2) {
      try {
        messages.add(MessageProtos.Envelope.parseFrom(queueItems.get(i)));
      } catch (final InvalidProtocolBufferException e) {
        throw new UncheckedIOException(e);
      }
    }

    return messages;
  }

  @Test
  void returnPresence() throws IOException {
    final UUID destinationUuid = UUID.randomUUID();
    final byte deviceId = 1;

    final MessagesCacheInsertScript insertScript =
        new MessagesCacheInsertScript(REDIS_CLUSTER_EXTENSION.getRedisCluster());

    assertFalse(insertScript.executeAsync(destinationUuid, deviceId, MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(UUID.randomUUID().toString())
        .build()).join());

    final FaultTolerantPubSubClusterConnection<byte[], byte[]> pubSubClusterConnection =
        REDIS_CLUSTER_EXTENSION.getRedisCluster().createBinaryPubSubConnection();

    pubSubClusterConnection.usePubSubConnection(connection ->
        connection.sync().ssubscribe(WebSocketConnectionEventManager.getClientEventChannel(destinationUuid, deviceId)));

    assertTrue(insertScript.executeAsync(destinationUuid, deviceId, MessageProtos.Envelope.newBuilder()
        .setServerTimestamp(Instant.now().getEpochSecond())
        .setServerGuid(UUID.randomUUID().toString())
        .build()).join());
  }
}
