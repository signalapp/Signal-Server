/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import java.util.Random;
import org.whispersystems.textsecuregcm.storage.Device;

public class DevicesHelper {

  private static final Random RANDOM = new Random();

  public static Device createDevice(final byte deviceId) {
    return createDevice(deviceId, 0);
  }

  public static Device createDevice(final byte deviceId, final long lastSeen) {
    return createDevice(deviceId, lastSeen, 0);
  }

  public static Device createDevice(final byte deviceId, final long lastSeen, final int registrationId) {
    final Device device = new Device();
    device.setId(deviceId);
    device.setLastSeen(lastSeen);
    device.setUserAgent("OWT");
    device.setRegistrationId(registrationId);

    return device;
  }
}
