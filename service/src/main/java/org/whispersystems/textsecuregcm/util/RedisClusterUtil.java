package org.whispersystems.textsecuregcm.util;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;

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
     * Returns a short Redis hash tag that maps to the same Redis cluster slot as the given key.
     *
     * @param key the key for which to find a matching hash tag
     * @return a Redis hash tag that maps to the same Redis cluster slot as the given key
     *
     * @see <a href="https://redis.io/topics/cluster-spec#keys-hash-tags">Redis Cluster Specification - Keys hash tags</a>
     */
    public static String getMinimalHashTag(final String key) {
        return HASHES_BY_SLOT[SlotHash.getSlot(key)];
    }
}
