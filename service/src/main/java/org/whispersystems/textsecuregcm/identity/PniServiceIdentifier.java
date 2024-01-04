/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.identity;

import io.swagger.v3.oas.annotations.media.Schema;

import org.signal.libsignal.protocol.ServiceId;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;

/**
 * An identifier for an account based on the account's phone number identifier (PNI).
 *
 * @param uuid the account's PNI UUID
 */
@Schema(
    type = "string",
    description = "An identifier for an account based on the account's phone number identifier (PNI)"
)
public record PniServiceIdentifier(UUID uuid) implements ServiceIdentifier {

  private static final IdentityType IDENTITY_TYPE = IdentityType.PNI;

  @Override
  public IdentityType identityType() {
    return IDENTITY_TYPE;
  }

  @Override
  public String toServiceIdentifierString() {
    return IDENTITY_TYPE.getStringPrefix() + uuid.toString();
  }

  @Override
  public byte[] toCompactByteArray() {
    return toFixedWidthByteArray();
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
  public ServiceId.Pni toLibsignal() {
    return new ServiceId.Pni(uuid);
  }

  public static PniServiceIdentifier valueOf(final String string) {
    if (!string.startsWith(IDENTITY_TYPE.getStringPrefix())) {
      throw new IllegalArgumentException("PNI account identifier did not start with \"PNI:\" prefix");
    }

    return new PniServiceIdentifier(UUID.fromString(string.substring(IDENTITY_TYPE.getStringPrefix().length())));
  }

  public static PniServiceIdentifier fromBytes(final byte[] bytes) {
    if (bytes.length == 17) {
      if (bytes[0] != IDENTITY_TYPE.getBytePrefix()) {
        throw new IllegalArgumentException("Unexpected byte array prefix: " + HexFormat.of().formatHex(new byte[] { bytes[0] }));
      }

      return new PniServiceIdentifier(UUIDUtil.fromBytes(Arrays.copyOfRange(bytes, 1, bytes.length)));
    }

    throw new IllegalArgumentException("Unexpected byte array length: " + bytes.length);
  }
}
