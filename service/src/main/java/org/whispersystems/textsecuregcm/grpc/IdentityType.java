/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;

public enum IdentityType {
  ACI,
  PNI;

  public static IdentityType fromGrpcIdentityType(final org.signal.chat.common.IdentityType grpcIdentityType) {
    return switch (grpcIdentityType) {
      case IDENTITY_TYPE_ACI -> ACI;
      case IDENTITY_TYPE_PNI -> PNI;
      case IDENTITY_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw Status.INVALID_ARGUMENT.asRuntimeException();
    };
  }
}
