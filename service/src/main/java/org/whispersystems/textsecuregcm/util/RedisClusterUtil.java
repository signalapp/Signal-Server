/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import io.lettuce.core.cluster.SlotHash;

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
}
