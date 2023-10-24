/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Context;
import io.grpc.Status;
import java.util.UUID;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.Device;

/**
 * Provides utility methods for working with authentication in the context of gRPC calls.
 */
public class AuthenticationUtil {

  static final Context.Key<UUID> CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY = Context.key("authenticated-aci");
  static final Context.Key<Byte> CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY = Context.key("authenticated-device-id");

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
    @Nullable final UUID accountIdentifier = CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY.get();
    @Nullable final Byte deviceId = CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY.get();

    if (accountIdentifier != null && deviceId != null) {
      return new AuthenticatedDevice(accountIdentifier, deviceId);
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
