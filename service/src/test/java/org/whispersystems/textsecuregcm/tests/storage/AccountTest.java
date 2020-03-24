package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccountTest {

  private final Device oldMasterDevice       = mock(Device.class);
  private final Device recentMasterDevice    = mock(Device.class);
  private final Device agingSecondaryDevice  = mock(Device.class);
  private final Device recentSecondaryDevice = mock(Device.class);
  private final Device oldSecondaryDevice    = mock(Device.class);

  private final Device uuidCapableDevice          = mock(Device.class);
  private final Device uuidIncapableDevice        = mock(Device.class);
  private final Device uuidIncapableExpiredDevice = mock(Device.class);

  @Before
  public void setup() {
    when(oldMasterDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldMasterDevice.isEnabled()).thenReturn(true);
    when(oldMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(recentMasterDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentMasterDevice.isEnabled()).thenReturn(true);
    when(recentMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(agingSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    when(agingSecondaryDevice.isEnabled()).thenReturn(false);
    when(agingSecondaryDevice.getId()).thenReturn(2L);

    when(recentSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentSecondaryDevice.isEnabled()).thenReturn(true);
    when(recentSecondaryDevice.getId()).thenReturn(2L);

    when(oldSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldSecondaryDevice.isEnabled()).thenReturn(false);
    when(oldSecondaryDevice.getId()).thenReturn(2L);

    when(uuidCapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(true, true, true));
    when(uuidCapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(uuidCapableDevice.isEnabled()).thenReturn(true);

    when(uuidIncapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(false, false, false));
    when(uuidIncapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(uuidIncapableDevice.isEnabled()).thenReturn(true);

    when(uuidIncapableExpiredDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(false, false, false));
    when(uuidIncapableExpiredDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    when(uuidIncapableExpiredDevice.isEnabled()).thenReturn(false);
  }

  @Test
  public void testAccountActive() {
    Account recentAccount = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(recentMasterDevice);
      add(recentSecondaryDevice);
    }}, "1234".getBytes());

    assertTrue(recentAccount.isEnabled());

    Account oldSecondaryAccount = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(recentMasterDevice);
      add(agingSecondaryDevice);
    }}, "1234".getBytes());

    assertTrue(oldSecondaryAccount.isEnabled());

    Account agingPrimaryAccount = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(oldMasterDevice);
      add(agingSecondaryDevice);
    }}, "1234".getBytes());

    assertTrue(agingPrimaryAccount.isEnabled());
  }

  @Test
  public void testAccountInactive() {
    Account oldPrimaryAccount = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(oldMasterDevice);
      add(oldSecondaryDevice);
    }}, "1234".getBytes());

    assertFalse(oldPrimaryAccount.isEnabled());
  }

  @Test
  public void testCapabilities() {
    Account uuidCapable = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(uuidCapableDevice);
    }}, "1234".getBytes());

    Account uuidIncapable = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(uuidCapableDevice);
      add(uuidIncapableDevice);
    }}, "1234".getBytes());

    Account uuidCapableWithExpiredIncapable = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(uuidCapableDevice);
      add(uuidIncapableExpiredDevice);
    }}, "1234".getBytes());

    assertTrue(uuidCapable.isUuidAddressingSupported());
    assertFalse(uuidIncapable.isUuidAddressingSupported());
    assertTrue(uuidCapableWithExpiredIncapable.isUuidAddressingSupported());
  }

}
