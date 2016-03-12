package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.HashSet;
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

  @Before
  public void setup() {
    when(oldMasterDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldMasterDevice.isActive()).thenReturn(true);
    when(oldMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(recentMasterDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentMasterDevice.isActive()).thenReturn(true);
    when(recentMasterDevice.getId()).thenReturn(Device.MASTER_ID);

    when(agingSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(31));
    when(agingSecondaryDevice.isActive()).thenReturn(false);
    when(agingSecondaryDevice.getId()).thenReturn(2L);

    when(recentSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    when(recentSecondaryDevice.isActive()).thenReturn(true);
    when(recentSecondaryDevice.getId()).thenReturn(2L);

    when(oldSecondaryDevice.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(366));
    when(oldSecondaryDevice.isActive()).thenReturn(false);
    when(oldSecondaryDevice.getId()).thenReturn(2L);
  }

  @Test
  public void testAccountActive() {
    Account recentAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(recentMasterDevice);
      add(recentSecondaryDevice);
    }});

    assertTrue(recentAccount.isActive());

    Account oldSecondaryAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(recentMasterDevice);
      add(agingSecondaryDevice);
    }});

    assertTrue(oldSecondaryAccount.isActive());

    Account agingPrimaryAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(oldMasterDevice);
      add(agingSecondaryDevice);
    }});

    assertTrue(agingPrimaryAccount.isActive());
  }

  @Test
  public void testAccountInactive() {
    Account oldPrimaryAccount = new Account("+14152222222", new HashSet<Device>() {{
      add(oldMasterDevice);
      add(oldSecondaryDevice);
    }});

    assertFalse(oldPrimaryAccount.isActive());
  }

}
