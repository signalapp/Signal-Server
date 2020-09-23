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
    public void testIsGroupsV2Supported(final String gcmId, final boolean gv2Capability, final boolean gv2_2Capability, final boolean expectGv2Supported) {
        final Device.DeviceCapabilities capabilities = new Device.DeviceCapabilities(gv2Capability, gv2_2Capability, false, false);
        final Device                    device       = new Device(1, "test", "auth-token", "salt", "signaling-key", gcmId, "apn-id", "apn-voip-id", false, 1, null, 0, 0, "user-agent", 0, capabilities);

        assertEquals(expectGv2Supported, device.isGroupsV2Supported());
    }

    private static Object argumentsForTestIsGroupsV2Supported() {
        return new Object[] {
                new Object[] { "gcm-id", false, false, false },
                new Object[] { "gcm-id", true,  false, true  },
                new Object[] { "gcm-id", false, true,  true  },
                new Object[] { "gcm-id", true,  true,  true  },
                new Object[] { null,     false, false, false },
                new Object[] { null,     true,  false, false },
                new Object[] { null,     false, true,  true  },
                new Object[] { null,     true,  true,  true  }
        };
    }
}
