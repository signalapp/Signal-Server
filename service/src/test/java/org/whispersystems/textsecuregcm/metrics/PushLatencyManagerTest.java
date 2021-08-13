/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class PushLatencyManagerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  void testGetLatency() throws ExecutionException, InterruptedException {

    final PushLatencyManager pushLatencyManager = new PushLatencyManager(REDIS_CLUSTER_EXTENSION.getRedisCluster());
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;
    final long expectedLatency = 1234;
    final long pushSentTimestamp = System.currentTimeMillis();
    final long clearQueueTimestamp = pushSentTimestamp + expectedLatency;

    assertNull(pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, System.currentTimeMillis()).get());

    {
      pushLatencyManager.recordPushSent(accountUuid, deviceId, pushSentTimestamp);

      assertEquals(expectedLatency,
          (long) pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, clearQueueTimestamp).get());
      assertNull(
          pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, System.currentTimeMillis()).get());
    }
  }
}
