package org.whispersystems.textsecuregcm.util;

import io.lettuce.core.cluster.SlotHash;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RedisClusterUtilTest {

    @Test
    public void testGetMinimalHashTag() {
        for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
            assertEquals(slot, SlotHash.getSlot(RedisClusterUtil.getMinimalHashTag(slot)));
        }
    }
}
