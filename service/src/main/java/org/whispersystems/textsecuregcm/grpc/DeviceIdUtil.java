/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;

public class DeviceIdUtil {

  static byte validate(int deviceId) {
    if (deviceId > Byte.MAX_VALUE) {
      throw Status.INVALID_ARGUMENT.withDescription("Device ID is out of range").asRuntimeException();
    }
    return (byte) deviceId;
  }
}
