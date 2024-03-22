/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Context;
import io.grpc.Status;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.Device;

/**
 * Provides utility methods for working with authentication in the context of gRPC calls.
 */
public class AuthenticationUtil {

  static final Context.Key<AuthenticatedDevice> CONTEXT_AUTHENTICATED_DEVICE = Context.key("authenticated-device");

  /**
   * Returns the account/device authenticated in the current gRPC context or throws an "unauthenticated" exception if
   * no authenticated account/device is available.
   *
   * @return the account/device identifier authenticated in the current gRPC context
   *
   * @throws io.grpc.StatusRuntimeException with a status of {@code UNAUTHENTICATED} if no authenticated account/device
   * could be retrieved from the current gRPC context
   */
  public static AuthenticatedDevice requireAuthenticatedDevice() {
    @Nullable final AuthenticatedDevice authenticatedDevice = CONTEXT_AUTHENTICATED_DEVICE.get();

    if (authenticatedDevice != null) {
      return authenticatedDevice;
    }

    throw Status.UNAUTHENTICATED.asRuntimeException();
  }

  /**
   * Returns the account/device authenticated in the current gRPC context or throws an "unauthenticated" exception if
   * no authenticated account/device is available or "permission denied" if the authenticated device is not the primary
   * device for the account.
   *
   * @return the account/device identifier authenticated in the current gRPC context
   *
   * @throws io.grpc.StatusRuntimeException with a status of {@code UNAUTHENTICATED} if no authenticated account/device
   * could be retrieved from the current gRPC context or a status of {@code PERMISSION_DENIED} if the authenticated
   * device is not the primary device for the authenticated account
   */
  public static AuthenticatedDevice requireAuthenticatedPrimaryDevice() {
    final AuthenticatedDevice authenticatedDevice = requireAuthenticatedDevice();

    if (authenticatedDevice.deviceId() != Device.PRIMARY_ID) {
      throw Status.PERMISSION_DENIED.asRuntimeException();
    }

    return authenticatedDevice;
  }
}
