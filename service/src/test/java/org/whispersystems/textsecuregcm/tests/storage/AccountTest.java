package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.Collections;
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

    when(uuidCapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(true, true, true, true));
    when(uuidCapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(uuidCapableDevice.isEnabled()).thenReturn(true);

    when(uuidIncapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(false, false, false, false));
    when(uuidIncapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(uuidIncapableDevice.isEnabled()).thenReturn(true);

    when(uuidIncapableExpiredDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(false, false, false, false));
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

  @Test
  public void testIsTransferSupported() {
    final Device                    transferCapableMasterDevice    = mock(Device.class);
    final Device                    nonTransferCapableMasterDevice = mock(Device.class);
    final Device                    transferCapableLinkedDevice    = mock(Device.class);

    final Device.DeviceCapabilities transferCapabilities           = mock(Device.DeviceCapabilities.class);
    final Device.DeviceCapabilities nonTransferCapabilities        = mock(Device.DeviceCapabilities.class);

    when(transferCapableMasterDevice.getId()).thenReturn(1L);
    when(transferCapableMasterDevice.isMaster()).thenReturn(true);
    when(transferCapableMasterDevice.getCapabilities()).thenReturn(transferCapabilities);

    when(nonTransferCapableMasterDevice.getId()).thenReturn(1L);
    when(nonTransferCapableMasterDevice.isMaster()).thenReturn(true);
    when(nonTransferCapableMasterDevice.getCapabilities()).thenReturn(nonTransferCapabilities);

    when(transferCapableLinkedDevice.getId()).thenReturn(2L);
    when(transferCapableLinkedDevice.isMaster()).thenReturn(false);
    when(transferCapableLinkedDevice.getCapabilities()).thenReturn(transferCapabilities);

    when(transferCapabilities.isTransfer()).thenReturn(true);
    when(nonTransferCapabilities.isTransfer()).thenReturn(false);

    {
      final Account transferableMasterAccount =
              new Account("+14152222222", UUID.randomUUID(), Collections.singleton(transferCapableMasterDevice), "1234".getBytes());

      assertTrue(transferableMasterAccount.isTransferSupported());
    }

    {
      final Account nonTransferableMasterAccount =
              new Account("+14152222222", UUID.randomUUID(), Collections.singleton(nonTransferCapableMasterDevice), "1234".getBytes());

      assertFalse(nonTransferableMasterAccount.isTransferSupported());
    }

    {
      final Account transferableLinkedAccount = new Account("+14152222222", UUID.randomUUID(), new HashSet<>() {{
        add(nonTransferCapableMasterDevice);
        add(transferCapableLinkedDevice);
      }}, "1234".getBytes());

      assertFalse(transferableLinkedAccount.isTransferSupported());
    }
  }

  @Test
  public void testDiscoverableByPhoneNumber() {
    final Account account = new Account("+14152222222", UUID.randomUUID(), Collections.singleton(recentMasterDevice), "1234".getBytes());

    assertTrue("Freshly-loaded legacy accounts should be discoverable by phone number.", account.isDiscoverableByPhoneNumber());

    account.setDiscoverableByPhoneNumber(false);
    assertFalse(account.isDiscoverableByPhoneNumber());

    account.setDiscoverableByPhoneNumber(true);
    assertTrue(account.isDiscoverableByPhoneNumber());
  }
}
