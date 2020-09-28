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
    public void testIsGroupsV2Supported(final boolean master, final String apnId, final boolean gv2Capability, final boolean gv2_2Capability, final boolean gv2_3Capability, final boolean expectGv2Supported) {
        final Device.DeviceCapabilities capabilities = new Device.DeviceCapabilities(gv2Capability, gv2_2Capability, gv2_3Capability, false, false);
        final Device                    device       = new Device(master ? 1 : 2, "test", "auth-token", "salt", "signaling-key", null, apnId, null, false, 1, null, 0, 0, "user-agent", 0, capabilities);

        assertEquals(expectGv2Supported, device.isGroupsV2Supported());
    }

    private static Object argumentsForTestIsGroupsV2Supported() {
        return new Object[] {
                //             master apnId     gv2    gv2-2  gv2-3  capable

                // Android master
                new Object[] { true,  null,     false, false, false, false },
                new Object[] { true,  null,     true,  false, false, true  },
                new Object[] { true,  null,     false, true,  false, true  },
                new Object[] { true,  null,     true,  true,  false, true  },
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
