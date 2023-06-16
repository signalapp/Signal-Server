/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.Context;
import java.util.Optional;
import java.util.UUID;

/**
 * Provides utility methods for working with authentication in the context of gRPC calls.
 */
public class AuthenticationUtil {

  static final Context.Key<UUID> CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY = Context.key("authenticated-aci");
  static final Context.Key<Long> CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY = Context.key("authenticated-device-id");

  public static Optional<UUID> getAuthenticatedAccountIdentifier() {
    return Optional.ofNullable(CONTEXT_AUTHENTICATED_ACCOUNT_IDENTIFIER_KEY.get());
  }

  public static Optional<Long> getAuthenticatedDeviceIdentifier() {
    return Optional.ofNullable(CONTEXT_AUTHENTICATED_DEVICE_IDENTIFIER_KEY.get());
  }
}
