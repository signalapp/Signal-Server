/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.identity;

import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.media.Schema;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import org.signal.libsignal.protocol.ServiceId;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

/**
 * An identifier for an account based on the account's ACI.
 *
 * @param uuid the account's ACI UUID
 */
@Schema(
    type = "string",
    description = "An identifier for an account based on the account's ACI"
)
public record AciServiceIdentifier(UUID uuid) implements ServiceIdentifier {
  private static final IdentityType IDENTITY_TYPE = IdentityType.ACI;

  @Override
  public IdentityType identityType() {
    return IDENTITY_TYPE;
  }

  @Override
  public String toServiceIdentifierString() {
    return uuid.toString();
  }

  @Override
  public byte[] toCompactByteArray() {
    return UUIDUtil.toBytes(uuid);
  }

  @Override
  public byte[] toFixedWidthByteArray() {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(17);
    byteBuffer.put(IDENTITY_TYPE.getBytePrefix());
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());
    byteBuffer.flip();

    return byteBuffer.array();
  }

  @Override
  public ServiceId.Aci toLibsignal() {
    return new ServiceId.Aci(uuid);
  }

  public static AciServiceIdentifier valueOf(final String string) {
    return new AciServiceIdentifier(UUID.fromString(string));
  }

  public static AciServiceIdentifier fromBytes(final byte[] bytes) {
    final UUID uuid;

    if (bytes.length == 17) {
      if (bytes[0] != IDENTITY_TYPE.getBytePrefix()) {
        throw new IllegalArgumentException("Unexpected byte array prefix: " + HexFormat.of().formatHex(new byte[] { bytes[0] }));
      }

      uuid = UUIDUtil.fromBytes(Arrays.copyOfRange(bytes, 1, bytes.length));
    } else {
      uuid = UUIDUtil.fromBytes(bytes);
    }

    return new AciServiceIdentifier(uuid);
  }
}
