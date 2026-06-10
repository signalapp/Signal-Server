/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import java.util.UUID;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class GrpcServiceIdentifierUtil {

  private GrpcServiceIdentifierUtil() {
  }

  public static ServiceIdentifier fromGrpcServiceIdentifier(final org.signal.chat.common.ServiceIdentifier serviceIdentifier) {
    if (serviceIdentifier == null) {
      throw GrpcExceptions.invalidArguments("invalid service identifier");
    }

    final UUID uuid;
    try {
      uuid = UUIDUtil.fromByteString(serviceIdentifier.getUuid());
    } catch (final IllegalArgumentException e) {
      throw GrpcExceptions.invalidArguments("invalid service identifier");
    }

    return switch (IdentityTypeUtil.fromGrpcIdentityType(serviceIdentifier.getIdentityType())) {
      case ACI -> new AciServiceIdentifier(uuid);
      case PNI -> new PniServiceIdentifier(uuid);
    };
  }

  public static org.signal.chat.common.ServiceIdentifier toGrpcServiceIdentifier(final ServiceIdentifier serviceIdentifier) {
    return org.signal.chat.common.ServiceIdentifier.newBuilder()
        .setIdentityType(IdentityTypeUtil.toGrpcIdentityType(serviceIdentifier.identityType()))
        .setUuid(UUIDUtil.toByteString(serviceIdentifier.uuid()))
        .build();
  }
}
