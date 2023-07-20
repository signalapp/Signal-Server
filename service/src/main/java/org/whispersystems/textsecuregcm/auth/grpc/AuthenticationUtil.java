/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Context;
import io.grpc.Status;
import java.util.UUID;
import javax.annotation.Nullable;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Provides utility methods for working with authentication in the context of gRPC calls.
 */
public class AuthenticationUtil {

  static final Context.Key<UUID> CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY = Context.key("authenticated-aci");
  static final Context.Key<Long> CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY = Context.key("authenticated-device-id");

  /**
   * Returns the account/device authenticated in the current gRPC context or throws an "unauthenticated" exception if
   * no authenticated account/device is available.
   *
   * @return the account/device authenticated in the current gRPC context
   *
   * @throws io.grpc.StatusRuntimeException with a status of {@code UNAUTHENTICATED} if no authenticated account/device
   * could be retrieved from the current gRPC context
   */
  public static AuthenticatedDevice requireAuthenticatedDevice() {
    @Nullable final UUID accountIdentifier = CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY.get();
    @Nullable final Long deviceId = CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY.get();

    if (accountIdentifier != null && deviceId != null) {
      return new AuthenticatedDevice(accountIdentifier, deviceId);
    }

    throw Status.UNAUTHENTICATED.asRuntimeException();
  }
}
