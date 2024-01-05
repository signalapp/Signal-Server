/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Device;

class RemoveExpiredLinkedDevicesCommandTest {

  public static Stream<Arguments> getDeviceIdsToRemove() {
    final Device primary = device(Device.PRIMARY_ID, false);

    final byte expiredDevice2Id = 2;
    final Device expiredDevice2 = device(expiredDevice2Id, true);

    final byte deviceId3 = 3;
    final Device device3 = device(deviceId3, false);

    final Device expiredPrimary = device(Device.PRIMARY_ID, true);

    return Stream.of(
        Arguments.of(List.of(primary), Set.of()),
        Arguments.of(List.of(primary, expiredDevice2), Set.of(expiredDevice2Id)),
        Arguments.of(List.of(primary, expiredDevice2, device3), Set.of(expiredDevice2Id)),
        Arguments.of(List.of(expiredPrimary, expiredDevice2, device3), Set.of(expiredDevice2Id))
    );
  }

  private static Device device(byte id, boolean expired) {
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(id);
    when(device.isExpired()).thenReturn(expired);
    when(device.isPrimary()).thenCallRealMethod();
    return device;
  }

  @ParameterizedTest
  @MethodSource
  void getDeviceIdsToRemove(final List<Device> devices, final Set<Byte> expectedIds) {
    assertEquals(expectedIds, RemoveExpiredLinkedDevicesCommand.getExpiredLinkedDeviceIds(devices));
  }
}
