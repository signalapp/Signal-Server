/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import java.util.UUID;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ServiceIdentifierUtil {

  private ServiceIdentifierUtil() {
  }

  public static ServiceIdentifier fromGrpcServiceIdentifier(final org.signal.chat.common.ServiceIdentifier serviceIdentifier) {
    final UUID uuid;

    try {
      uuid = UUIDUtil.fromByteString(serviceIdentifier.getUuid());
    } catch (final IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT.asRuntimeException();
    }

    return switch (IdentityTypeUtil.fromGrpcIdentityType(serviceIdentifier.getIdentityType())) {
      case ACI -> new AciServiceIdentifier(uuid);
      case PNI -> new PniServiceIdentifier(uuid);
    };
  }
}
