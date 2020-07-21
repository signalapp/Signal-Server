package org.whispersystems.textsecuregcm.workers;

import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ClearCacheClusterCommandTest extends AbstractRedisClusterTest {

    private static final int KEY_COUNT = 10_000;

    @Test
    public void testClearCache() throws InterruptedException {
        final FaultTolerantRedisCluster cluster = getRedisCluster();

        cluster.useWriteCluster(connection -> {
            final RedisAdvancedClusterCommands<String, String> clusterCommands = connection.sync();

            for (int i = 0; i < KEY_COUNT; i++) {
                clusterCommands.set("key::" + i, String.valueOf(i));
            }
        });

        {
            final AtomicInteger nodeCount = new AtomicInteger(0);

            cluster.useWriteCluster(connection -> connection.sync().masters().asMap().forEach((node, commands) -> {
                assertTrue(commands.dbsize() > 0);
                nodeCount.incrementAndGet();
            }));

            assertTrue(nodeCount.get() > 0);
        }

        ClearCacheClusterCommand.clearCache(cluster);

        Thread.sleep(1000);

        {
            final AtomicInteger nodeCount = new AtomicInteger(0);

            cluster.useWriteCluster(connection -> connection.sync().masters().asMap().forEach((node, commands) -> {
                assertEquals(0L, (long)commands.dbsize());
                nodeCount.incrementAndGet();
            }));

            assertTrue(nodeCount.get() > 0);
        }
    }
}
