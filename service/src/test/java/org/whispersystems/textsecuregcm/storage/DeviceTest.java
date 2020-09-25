package org.whispersystems.textsecuregcm.storage;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class DeviceTest {

    @Test
    @Parameters(method = "argumentsForTestIsGroupsV2Supported")
    public void testIsGroupsV2Supported(final String gcmId, final String apnId, final boolean gv2Capability, final boolean gv2_2Capability, final boolean gv2_3Capability, final boolean expectGv2Supported) {
        final Device.DeviceCapabilities capabilities = new Device.DeviceCapabilities(gv2Capability, gv2_2Capability, gv2_3Capability, false, false);
        final Device                    device       = new Device(1, "test", "auth-token", "salt", "signaling-key", gcmId, apnId, null, false, 1, null, 0, 0, "user-agent", 0, capabilities);

        assertEquals(expectGv2Supported, device.isGroupsV2Supported());
    }

    private static Object argumentsForTestIsGroupsV2Supported() {
        return new Object[] {
                //             gcmId     apnId     gv2    gv2-2  gv2-3  capable
                new Object[] { "gcm-id", null,     false, false, false, false },
                new Object[] { "gcm-id", null,     true,  false, false, true  },
                new Object[] { "gcm-id", null,     false, true,  false, true  },
                new Object[] { "gcm-id", null,     true,  true,  false, true  },
                new Object[] { "gcm-id", null,     false, false, true,  true  },
                new Object[] { "gcm-id", null,     true,  false, true,  true  },
                new Object[] { "gcm-id", null,     false, true,  true,  true  },
                new Object[] { "gcm-id", null,     true,  true,  true,  true  },
                new Object[] { null,     "apn-id", false, false, false, false },
                new Object[] { null,     "apn-id", true,  false, false, false },
                new Object[] { null,     "apn-id", false, true,  false, true  },
                new Object[] { null,     "apn-id", true,  true,  false, true  },
                new Object[] { null,     "apn-id", false, false, true,  true  },
                new Object[] { null,     "apn-id", true,  false, true,  true  },
                new Object[] { null,     "apn-id", false, true,  true,  true  },
                new Object[] { null,     "apn-id", true,  true,  true,  true  },
                new Object[] { null,     null,     false, false, false, false },
                new Object[] { null,     null,     true,  false, false, false },
                new Object[] { null,     null,     false, true,  false, false },
                new Object[] { null,     null,     true,  true,  false, false },
                new Object[] { null,     null,     false, false, true,  true  },
                new Object[] { null,     null,     true,  false, true,  true  },
                new Object[] { null,     null,     false, true,  true,  true  },
                new Object[] { null,     null,     true,  true,  true,  true  }
        };
    }
}
