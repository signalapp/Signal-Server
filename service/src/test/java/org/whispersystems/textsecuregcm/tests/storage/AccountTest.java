/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

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

  private final Device gv1MigrationCapableDevice = mock(Device.class);
  private final Device gv1MigrationIncapableDevice        = mock(Device.class);
  private final Device gv1MigrationIncapableExpiredDevice = mock(Device.class);

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

    when(gv2CapableDevice.isGroupsV2Supported()).thenReturn(true);
    when(gv2CapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(gv2CapableDevice.isEnabled()).thenReturn(true);

    when(gv2IncapableDevice.isGroupsV2Supported()).thenReturn(false);
    when(gv2IncapableDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(gv2IncapableDevice.isEnabled()).thenReturn(true);

    when(gv2IncapableExpiredDevice.isGroupsV2Supported()).thenReturn(false);
    when(gv2IncapableExpiredDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    when(gv2IncapableExpiredDevice.isEnabled()).thenReturn(false);

    when(gv1MigrationCapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(true, true, true, true, true, true));
    when(gv1MigrationIncapableDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(true, true, true, true, true, false));
    when(gv1MigrationIncapableExpiredDevice.getCapabilities()).thenReturn(new Device.DeviceCapabilities(true, true, true, true, true, false));
  }

  @Test
  public void testIsEnabled() {
    final Device enabledMasterDevice  = mock(Device.class);
    final Device enabledLinkedDevice  = mock(Device.class);
    final Device disabledMasterDevice = mock(Device.class);
    final Device disabledLinkedDevice = mock(Device.class);

    when(enabledMasterDevice.isEnabled()).thenReturn(true);
    when(enabledLinkedDevice.isEnabled()).thenReturn(true);
    when(disabledMasterDevice.isEnabled()).thenReturn(false);
    when(disabledLinkedDevice.isEnabled()).thenReturn(false);

    when(enabledMasterDevice.getId()).thenReturn(1L);
    when(enabledLinkedDevice.getId()).thenReturn(2L);
    when(disabledMasterDevice.getId()).thenReturn(1L);
    when(disabledLinkedDevice.getId()).thenReturn(2L);

    assertTrue( new Account("+14151234567", UUID.randomUUID(), Set.of(enabledMasterDevice),                        new byte[0]).isEnabled());
    assertTrue( new Account("+14151234567", UUID.randomUUID(), Set.of(enabledMasterDevice, enabledLinkedDevice),   new byte[0]).isEnabled());
    assertTrue( new Account("+14151234567", UUID.randomUUID(), Set.of(enabledMasterDevice, disabledLinkedDevice),  new byte[0]).isEnabled());
    assertFalse(new Account("+14151234567", UUID.randomUUID(), Set.of(disabledMasterDevice),                       new byte[0]).isEnabled());
    assertFalse(new Account("+14151234567", UUID.randomUUID(), Set.of(disabledMasterDevice, enabledLinkedDevice),  new byte[0]).isEnabled());
    assertFalse(new Account("+14151234567", UUID.randomUUID(), Set.of(disabledMasterDevice, disabledLinkedDevice), new byte[0]).isEnabled());
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
    assertTrue(new Account("+18005551234", UUID.randomUUID(), Set.of(gv2CapableDevice), "1234".getBytes(StandardCharsets.UTF_8)).isGroupsV2Supported());
    assertTrue(new Account("+18005551234", UUID.randomUUID(), Set.of(gv2CapableDevice, gv2IncapableExpiredDevice), "1234".getBytes(StandardCharsets.UTF_8)).isGroupsV2Supported());
    assertFalse(new Account("+18005551234", UUID.randomUUID(), Set.of(gv2CapableDevice, gv2IncapableDevice), "1234".getBytes(StandardCharsets.UTF_8)).isGroupsV2Supported());
  }

  @Test
  public void isGv1MigrationSupported() {
    assertTrue(new Account("+18005551234", UUID.randomUUID(), Set.of(gv1MigrationCapableDevice), "1234".getBytes(StandardCharsets.UTF_8)).isGv1MigrationSupported());
    assertFalse(new Account("+18005551234", UUID.randomUUID(), Set.of(gv1MigrationCapableDevice, gv1MigrationIncapableDevice), "1234".getBytes(StandardCharsets.UTF_8)).isGv1MigrationSupported());
    assertFalse(new Account("+18005551234", UUID.randomUUID(), Set.of(gv1MigrationCapableDevice, gv1MigrationIncapableExpiredDevice), "1234".getBytes(StandardCharsets.UTF_8)).isGv1MigrationSupported());
  }
}
