/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lettuce.core.cluster.SlotHash;
import org.junit.jupiter.api.Test;

class RedisClusterUtilTest {

    @Test
    void testGetMinimalHashTag() {
        for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
            assertEquals(slot, SlotHash.getSlot(RedisClusterUtil.getMinimalHashTag(slot)));
        }
    }
}
