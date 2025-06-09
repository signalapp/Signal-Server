/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;

public class DeviceCapabilityUtil {

  private DeviceCapabilityUtil() {
  }

  public static DeviceCapability fromGrpcDeviceCapability(final org.signal.chat.common.DeviceCapability grpcDeviceCapability) {
    return switch (grpcDeviceCapability) {
      case DEVICE_CAPABILITY_STORAGE -> DeviceCapability.STORAGE;
      case DEVICE_CAPABILITY_TRANSFER -> DeviceCapability.TRANSFER;
      case DEVICE_CAPABILITY_DELETE_SYNC -> DeviceCapability.DELETE_SYNC;
      case DEVICE_CAPABILITY_STORAGE_SERVICE_RECORD_KEY_ROTATION -> DeviceCapability.STORAGE_SERVICE_RECORD_KEY_ROTATION;
      case DEVICE_CAPABILITY_ATTACHMENT_BACKFILL -> DeviceCapability.ATTACHMENT_BACKFILL;
      case DEVICE_CAPABILITY_SPARSE_POST_QUANTUM_RATCHET -> DeviceCapability.SPARSE_POST_QUANTUM_RATCHET;
      case DEVICE_CAPABILITY_UNSPECIFIED, UNRECOGNIZED -> throw Status.INVALID_ARGUMENT.withDescription("Unrecognized device capability").asRuntimeException();
    };
  }

  public static org.signal.chat.common.DeviceCapability toGrpcDeviceCapability(final DeviceCapability deviceCapability) {
    return switch (deviceCapability) {
      case STORAGE -> org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_STORAGE;
      case TRANSFER -> org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_TRANSFER;
      case DELETE_SYNC -> org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_DELETE_SYNC;
      case STORAGE_SERVICE_RECORD_KEY_ROTATION -> org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_STORAGE_SERVICE_RECORD_KEY_ROTATION;
      case ATTACHMENT_BACKFILL -> org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_ATTACHMENT_BACKFILL;
      case SPARSE_POST_QUANTUM_RATCHET -> org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_SPARSE_POST_QUANTUM_RATCHET;
    };
  }
}
