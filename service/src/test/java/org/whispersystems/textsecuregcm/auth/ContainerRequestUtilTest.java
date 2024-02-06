/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContainerRequestUtilTest {

  @Test
  void testBuildDevicesEnabled() {

    final byte disabledDeviceId = 3;

    final Account account = mock(Account.class);

    final List<Device> devices = new ArrayList<>();
    when(account.getDevices()).thenReturn(devices);

    IntStream.range(1, 5)
        .forEach(id -> {
          final Device device = mock(Device.class);
          when(device.getId()).thenReturn((byte) id);
          when(device.isEnabled()).thenReturn(id != disabledDeviceId);
          devices.add(device);
        });

    final Map<Byte, Boolean> devicesEnabled = ContainerRequestUtil.AccountInfo.fromAccount(account).devicesEnabled();

    assertEquals(4, devicesEnabled.size());

    assertAll(devicesEnabled.entrySet().stream()
        .map(deviceAndEnabled -> () -> {
          if (deviceAndEnabled.getKey().equals(disabledDeviceId)) {
            assertFalse(deviceAndEnabled.getValue());
          } else {
            assertTrue(deviceAndEnabled.getValue());
          }
        }));
  }
}
