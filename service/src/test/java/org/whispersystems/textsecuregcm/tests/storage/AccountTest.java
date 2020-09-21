package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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

  private final Device gv2CapableDevice          = mock(Device.class);
  private final Device gv2IncapableDevice        = mock(Device.class);
  private final Device gv2IncapableExpiredDevice = mock(Device.class);

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

    when(gv2CapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(true, false, true, true));
    when(gv2CapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(gv2CapableDevice.isEnabled()).thenReturn(true);

    when(gv2IncapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(false, false, false, false));
    when(gv2IncapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(gv2IncapableDevice.isEnabled()).thenReturn(true);

    when(gv2IncapableExpiredDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(false, false, false, false));
    when(gv2IncapableExpiredDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    when(gv2IncapableExpiredDevice.isEnabled()).thenReturn(false);
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
      add(gv2CapableDevice);
    }}, "1234".getBytes());

    Account uuidIncapable = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(gv2CapableDevice);
      add(gv2IncapableDevice);
    }}, "1234".getBytes());

    Account uuidCapableWithExpiredIncapable = new Account("+14152222222", UUID.randomUUID(), new HashSet<Device>() {{
      add(gv2CapableDevice);
      add(gv2IncapableExpiredDevice);
    }}, "1234".getBytes());

    assertTrue(uuidCapable.isGroupsV2Supported());
    assertFalse(uuidIncapable.isGroupsV2Supported());
    assertTrue(uuidCapableWithExpiredIncapable.isGroupsV2Supported());
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

  @Test
  public void isGroupsV2Supported() {
    {
      final Device                    gv2CapableDevice       = mock(Device.class);
      final Device                    secondGv2CapableDevice = mock(Device.class);
      final Device.DeviceCapabilities gv2Capabilities        = mock(Device.DeviceCapabilities.class);
      final Device.DeviceCapabilities secondGv2Capabilities  = mock(Device.DeviceCapabilities.class);

      when(gv2CapableDevice.isEnabled()).thenReturn(true);
      when(gv2CapableDevice.getCapabilities()).thenReturn(gv2Capabilities);
      when(gv2Capabilities.isGv2()).thenReturn(true);

      when(secondGv2CapableDevice.isEnabled()).thenReturn(true);
      when(secondGv2CapableDevice.getCapabilities()).thenReturn(secondGv2Capabilities);
      when(secondGv2Capabilities.isGv2()).thenReturn(true);

      final Account account = new Account("+18005551234", UUID.randomUUID(), Set.of(gv2CapableDevice, secondGv2CapableDevice), "1234".getBytes(StandardCharsets.UTF_8));

      assertTrue(account.isGroupsV2Supported());
    }

    {
      final Device                    gv2CapableDevice    = mock(Device.class);
      final Device                    nonGv2CapableDevice = mock(Device.class);
      final Device.DeviceCapabilities gv2Capabilities     = mock(Device.DeviceCapabilities.class);
      final Device.DeviceCapabilities nonGv2Capabilities  = mock(Device.DeviceCapabilities.class);

      when(gv2CapableDevice.isEnabled()).thenReturn(true);
      when(gv2CapableDevice.getCapabilities()).thenReturn(gv2Capabilities);
      when(gv2Capabilities.isGv2()).thenReturn(true);

      when(nonGv2CapableDevice.isEnabled()).thenReturn(true);
      when(nonGv2CapableDevice.getCapabilities()).thenReturn(nonGv2Capabilities);
      when(nonGv2Capabilities.isGv2()).thenReturn(false);

      final Account account = new Account("+18005551234", UUID.randomUUID(), Set.of(gv2CapableDevice, nonGv2CapableDevice), "1234".getBytes(StandardCharsets.UTF_8));

      assertFalse(account.isGroupsV2Supported());
    }

    {
      final Device                    iosGv2Device      = mock(Device.class);
      final Device                    iosGv2_2Device    = mock(Device.class);
      final Device.DeviceCapabilities gv2Capabilities   = mock(Device.DeviceCapabilities.class);
      final Device.DeviceCapabilities gv2_2Capabilities = mock(Device.DeviceCapabilities.class);

      when(iosGv2Device.getApnId()).thenReturn("apn-id");
      when(iosGv2Device.isEnabled()).thenReturn(true);
      when(iosGv2Device.getCapabilities()).thenReturn(gv2Capabilities);
      when(gv2Capabilities.isGv2()).thenReturn(true);
      when(gv2Capabilities.isGv2_2()).thenReturn(false);

      when(iosGv2Device.getApnId()).thenReturn("different-apn-id");
      when(iosGv2_2Device.isEnabled()).thenReturn(true);
      when(iosGv2_2Device.getCapabilities()).thenReturn(gv2_2Capabilities);
      when(gv2_2Capabilities.isGv2()).thenReturn(true);
      when(gv2_2Capabilities.isGv2_2()).thenReturn(true);

      assertFalse(new Account("+18005551234", UUID.randomUUID(), Set.of(iosGv2Device, iosGv2_2Device), "1234".getBytes(StandardCharsets.UTF_8)).isGroupsV2Supported());
      assertTrue(new Account("+18005551234", UUID.randomUUID(), Set.of(iosGv2_2Device), "1234".getBytes(StandardCharsets.UTF_8)).isGroupsV2Supported());
    }
  }
}
