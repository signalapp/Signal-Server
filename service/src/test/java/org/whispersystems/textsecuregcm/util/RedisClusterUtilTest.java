package org.whispersystems.textsecuregcm.util;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.sync.Executions;
import io.lettuce.core.cluster.api.sync.NodeSelection;
import io.lettuce.core.cluster.api.sync.NodeSelectionCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import redis.embedded.Redis;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class RedisClusterUtilTest {

    @Test
    public void testGetMinimalHashTag() {
        for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
            assertEquals(slot, SlotHash.getSlot(RedisClusterUtil.getMinimalHashTag(slot)));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    @Parameters(method = "argumentsForTestAssertKeyspaceNotificationsConfigured")
    public void testAssertKeyspaceNotificationsConfigured(final String requiredKeyspaceNotifications, final String configuerdKeyspaceNotifications, final boolean expectException) {
        final RedisAdvancedClusterCommands<String, String> commands     = mock(RedisAdvancedClusterCommands.class);
        final FaultTolerantRedisCluster                    redisCluster = RedisClusterHelper.buildMockRedisCluster(commands);

        when(commands.configGet("notify-keyspace-events")).thenReturn(Map.of("notify-keyspace-events", configuerdKeyspaceNotifications));

        if (expectException) {
            try {
                RedisClusterUtil.assertKeyspaceNotificationsConfigured(redisCluster, requiredKeyspaceNotifications);
                fail("Expected IllegalStateException");
            } catch (final IllegalStateException ignored) {
            }
        } else {
            RedisClusterUtil.assertKeyspaceNotificationsConfigured(redisCluster, requiredKeyspaceNotifications);
        }
    }

    @SuppressWarnings("unused")
    private Object argumentsForTestAssertKeyspaceNotificationsConfigured() {
        return new Object[] {
                new Object[] { "K$gz", "",      true  },
                new Object[] { "K$gz", "K$gz",  false },
                new Object[] { "K$gz", "K$gzl", false },
                new Object[] { "K$gz", "KA",    false },
                new Object[] { "",     "A",     false },
                new Object[] { "",     "",      false },
        };
    }
}
