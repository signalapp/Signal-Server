package org.whispersystems.textsecuregcm.metrics;

import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class PushLatencyManagerTest extends AbstractRedisClusterTest {

    @Test
    public void testGetLatency() {
        final PushLatencyManager pushLatencyManager  = new PushLatencyManager(getRedisCluster());
        final UUID               accountUuid         = UUID.randomUUID();
        final long               deviceId            = 1;
        final long               expectedLatency     = 1234;
        final long               pushSentTimestamp   = System.currentTimeMillis();
        final long               clearQueueTimestamp = pushSentTimestamp + expectedLatency;

        assertEquals(Optional.empty(), pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, System.currentTimeMillis()));

        {
            pushLatencyManager.recordPushSent(accountUuid, deviceId, pushSentTimestamp);

            assertEquals(Optional.of(expectedLatency), pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, clearQueueTimestamp));
            assertEquals(Optional.empty(), pushLatencyManager.getLatencyAndClearTimestamp(accountUuid, deviceId, System.currentTimeMillis()));
        }
    }
}
