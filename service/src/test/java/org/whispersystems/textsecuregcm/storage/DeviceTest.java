/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;

import java.time.Duration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(JUnitParamsRunner.class)
public class DeviceTest {

    @Test
    @Parameters(method = "argumentsForTestIsEnabled")
    public void testIsEnabled(final boolean master, final boolean fetchesMessages, final String apnId, final String gcmId, final SignedPreKey signedPreKey, final Duration timeSinceLastSeen, final boolean expectEnabled) {
        final long lastSeen = System.currentTimeMillis() - timeSinceLastSeen.toMillis();
        final Device device = new Device(master ? 1 : 2, "test", "auth-token", "salt", "signaling-key", gcmId, apnId, null, fetchesMessages, 1, signedPreKey, lastSeen, lastSeen, "user-agent", 0, null);

        assertEquals(expectEnabled, device.isEnabled());
    }

    private static Object argumentsForTestIsEnabled() {
        return new Object[] {
                //             master fetchesMessages apnId     gcmId     signedPreKey              lastSeen             expectEnabled
                new Object[] { true,  false,          null,     null,     null,                     Duration.ofDays(60), false },
                new Object[] { true,  false,          null,     null,     null,                     Duration.ofDays(1),  false },
                new Object[] { true,  false,          null,     null,     mock(SignedPreKey.class), Duration.ofDays(60), false },
                new Object[] { true,  false,          null,     null,     mock(SignedPreKey.class), Duration.ofDays(1),  false },
                new Object[] { true,  false,          null,     "gcm-id", null,                     Duration.ofDays(60), false },
                new Object[] { true,  false,          null,     "gcm-id", null,                     Duration.ofDays(1),  false },
                new Object[] { true,  false,          null,     "gcm-id", mock(SignedPreKey.class), Duration.ofDays(60), true  },
                new Object[] { true,  false,          null,     "gcm-id", mock(SignedPreKey.class), Duration.ofDays(1),  true  },
                new Object[] { true,  false,          "apn-id", null,     null,                     Duration.ofDays(60), false },
                new Object[] { true,  false,          "apn-id", null,     null,                     Duration.ofDays(1),  false },
                new Object[] { true,  false,          "apn-id", null,     mock(SignedPreKey.class), Duration.ofDays(60), true  },
                new Object[] { true,  false,          "apn-id", null,     mock(SignedPreKey.class), Duration.ofDays(1),  true  },
                new Object[] { true,  true,           null,     null,     null,                     Duration.ofDays(60), false },
                new Object[] { true,  true,           null,     null,     null,                     Duration.ofDays(1),  false },
                new Object[] { true,  true,           null,     null,     mock(SignedPreKey.class), Duration.ofDays(60), true  },
                new Object[] { true,  true,           null,     null,     mock(SignedPreKey.class), Duration.ofDays(1),  true  },
                new Object[] { false, false,          null,     null,     null,                     Duration.ofDays(60), false },
                new Object[] { false, false,          null,     null,     null,                     Duration.ofDays(1),  false },
                new Object[] { false, false,          null,     null,     mock(SignedPreKey.class), Duration.ofDays(60), false },
                new Object[] { false, false,          null,     null,     mock(SignedPreKey.class), Duration.ofDays(1),  false },
                new Object[] { false, false,          null,     "gcm-id", null,                     Duration.ofDays(60), false },
                new Object[] { false, false,          null,     "gcm-id", null,                     Duration.ofDays(1),  false },
                new Object[] { false, false,          null,     "gcm-id", mock(SignedPreKey.class), Duration.ofDays(60), false },
                new Object[] { false, false,          null,     "gcm-id", mock(SignedPreKey.class), Duration.ofDays(1),  true  },
                new Object[] { false, false,          "apn-id", null,     null,                     Duration.ofDays(60), false },
                new Object[] { false, false,          "apn-id", null,     null,                     Duration.ofDays(1),  false },
                new Object[] { false, false,          "apn-id", null,     mock(SignedPreKey.class), Duration.ofDays(60), false },
                new Object[] { false, false,          "apn-id", null,     mock(SignedPreKey.class), Duration.ofDays(1),  true  },
                new Object[] { false, true,           null,     null,     null,                     Duration.ofDays(60), false },
                new Object[] { false, true,           null,     null,     null,                     Duration.ofDays(1),  false },
                new Object[] { false, true,           null,     null,     mock(SignedPreKey.class), Duration.ofDays(60), false },
                new Object[] { false, true,           null,     null,     mock(SignedPreKey.class), Duration.ofDays(1),  true  }
        };
    }

    @Test
    @Parameters(method = "argumentsForTestIsGroupsV2Supported")
    public void testIsGroupsV2Supported(final boolean master, final String apnId, final boolean gv2Capability, final boolean gv2_2Capability, final boolean gv2_3Capability, final boolean expectGv2Supported) {
        final Device.DeviceCapabilities capabilities = new Device.DeviceCapabilities(gv2Capability, gv2_2Capability, gv2_3Capability, false, false, false);
        final Device                    device       = new Device(master ? 1 : 2, "test", "auth-token", "salt", "signaling-key", null, apnId, null, false, 1, null, 0, 0, "user-agent", 0, capabilities);

        assertEquals(expectGv2Supported, device.isGroupsV2Supported());
    }

    private static Object argumentsForTestIsGroupsV2Supported() {
        return new Object[] {
                //             master apnId     gv2    gv2-2  gv2-3  capable

                // Android master
                new Object[] { true,  null,     false, false, false, false },
                new Object[] { true,  null,     true,  false, false, false },
                new Object[] { true,  null,     false, true,  false, false },
                new Object[] { true,  null,     true,  true,  false, false },
                new Object[] { true,  null,     false, false, true,  true  },
                new Object[] { true,  null,     true,  false, true,  true  },
                new Object[] { true,  null,     false, true,  true,  true  },
                new Object[] { true,  null,     true,  true,  true,  true  },

                // iOs master
                new Object[] { true,  "apn-id", false, false, false, false },
                new Object[] { true,  "apn-id", true,  false, false, false },
                new Object[] { true,  "apn-id", false, true,  false, true  },
                new Object[] { true,  "apn-id", true,  true,  false, true  },
                new Object[] { true,  "apn-id", false, false, true,  true  },
                new Object[] { true,  "apn-id", true,  false, true,  true  },
                new Object[] { true,  "apn-id", false, true,  true,  true  },
                new Object[] { true,  "apn-id", true,  true,  true,  true  },

                // iOs linked
                new Object[] { false, "apn-id", false, false, false, false },
                new Object[] { false, "apn-id", true,  false, false, false },
                new Object[] { false, "apn-id", false, true,  false, true  },
                new Object[] { false, "apn-id", true,  true,  false, true  },
                new Object[] { false, "apn-id", false, false, true,  true  },
                new Object[] { false, "apn-id", true,  false, true,  true  },
                new Object[] { false, "apn-id", false, true,  true,  true  },
                new Object[] { false, "apn-id", true,  true,  true,  true  },

                // desktop linked
                new Object[] { false, null,     false, false, false, false },
                new Object[] { false, null,     true,  false, false, false },
                new Object[] { false, null,     false, true,  false, false },
                new Object[] { false, null,     true,  true,  false, false },
                new Object[] { false, null,     false, false, true,  true  },
                new Object[] { false, null,     true,  false, true,  true  },
                new Object[] { false, null,     false, true,  true,  true  },
                new Object[] { false, null,     true,  true,  true,  true  }
        };
    }
}
