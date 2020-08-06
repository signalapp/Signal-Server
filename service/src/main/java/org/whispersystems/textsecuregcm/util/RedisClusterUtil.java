package org.whispersystems.textsecuregcm.util;

import io.lettuce.core.cluster.SlotHash;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class RedisClusterUtil {

    private static final String[] HASHES_BY_SLOT = new String[SlotHash.SLOT_COUNT];

    static {
        int slotsCovered = 0;
        int i = 0;

        while (slotsCovered < HASHES_BY_SLOT.length) {
            final String hash = Integer.toString(i++, 36);
            final int slot = SlotHash.getSlot(hash);

            if (HASHES_BY_SLOT[slot] == null) {
                HASHES_BY_SLOT[slot] = hash;
                slotsCovered += 1;
            }
        }
    }

    /**
     * Returns a Redis hash tag that maps to the given cluster slot.
     *
     * @param slot the Redis cluster slot for which to retrieve a hash tag
     *
     * @return a Redis hash tag that maps to the given cluster slot
     *
     * @see <a href="https://redis.io/topics/cluster-spec#keys-hash-tags">Redis Cluster Specification - Keys hash tags</a>
     */
    public static String getMinimalHashTag(final int slot) {
        return HASHES_BY_SLOT[slot];
    }

    /**
     * Asserts that a Redis cluster is configured to generate (at least) a specific set of keyspace notification events.
     *
     * @param redisCluster the Redis cluster to check for the required keyspace notification configuration
     * @param requiredKeyspaceNotifications a string representing the required keyspace notification events (e.g. "Kg$lz")
     *
     * @throws IllegalStateException if the given Redis cluster is not configured to generate the required keyspace
     * notification events
     *
     * @see <a href="https://redis.io/topics/notifications#configuration">Redis Keyspace Notifications - Configuration</a>
     */
    public static void assertKeyspaceNotificationsConfigured(final FaultTolerantRedisCluster redisCluster, final String requiredKeyspaceNotifications) {
        final String configuredKeyspaceNotifications = redisCluster.withReadCluster(connection -> connection.sync().configGet("notify-keyspace-events"))
                                                                   .getOrDefault("notify-keyspace-events", "")
                                                                   .replace("A", "g$lshztxe");

        for (final char requiredNotificationType : requiredKeyspaceNotifications.toCharArray()) {
            if (configuredKeyspaceNotifications.indexOf(requiredNotificationType) == -1) {
                throw new IllegalStateException(String.format("Required at least \"%s\" for keyspace notifications, but only had \"%s\".", requiredKeyspaceNotifications, configuredKeyspaceNotifications));
            }
        }
    }
}
