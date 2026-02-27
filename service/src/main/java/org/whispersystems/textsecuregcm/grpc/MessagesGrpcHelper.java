/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.signal.chat.messages.MismatchedDevices;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

public class MessagesGrpcHelper {

  /**
   * Translates an internal {@link org.whispersystems.textsecuregcm.controllers.MismatchedDevices} entity to a gRPC
   * {@link MismatchedDevices} entity.
   *
   * @param serviceIdentifier the service identifier to which the mismatched device response applies
   * @param mismatchedDevices the mismatched device entity to translate to gRPC
   *
   * @return a gRPC {@code MismatchedDevices} representation of the given mismatched devices
   */
  public static MismatchedDevices buildMismatchedDevices(final ServiceIdentifier serviceIdentifier,
      final org.whispersystems.textsecuregcm.controllers.MismatchedDevices mismatchedDevices) {

    final MismatchedDevices.Builder mismatchedDevicesBuilder = MismatchedDevices.newBuilder()
        .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier));

    mismatchedDevices.missingDeviceIds().forEach(mismatchedDevicesBuilder::addMissingDevices);
    mismatchedDevices.extraDeviceIds().forEach(mismatchedDevicesBuilder::addExtraDevices);
    mismatchedDevices.staleDeviceIds().forEach(mismatchedDevicesBuilder::addStaleDevices);

    return mismatchedDevicesBuilder.build();
  }
}
