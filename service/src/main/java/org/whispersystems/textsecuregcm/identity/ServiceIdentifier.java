/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.identity;

import com.google.protobuf.ByteString;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.UUID;
import org.signal.libsignal.protocol.ServiceId;

/**
 * A "service identifier" is a tuple of a UUID and identity type that identifies an account and identity within the
 * Signal service.
 */
@Schema(
    type = "string",
    description = "A service identifier is a tuple of a UUID and identity type that identifies an account and identity within the Signal service.",
    subTypes = {AciServiceIdentifier.class, PniServiceIdentifier.class}
)
public sealed interface ServiceIdentifier permits AciServiceIdentifier, PniServiceIdentifier {

  /**
   * Returns the identity type of this account identifier.
   *
   * @return the identity type of this account identifier
   */
  IdentityType identityType();

  /**
   * Returns the UUID for this account identifier.
   *
   * @return the UUID for this account identifier
   */
  UUID uuid();

  /**
   * Returns a string representation of this account identifier in a format that clients can unambiguously resolve into
   * an identity type and UUID.
   *
   * @return a "strongly-typed" string representation of this account identifier
   */
  String toServiceIdentifierString();

  /**
   * Returns a compact binary representation of this account identifier.
   *
   * @return a binary representation of this account identifier
   */
  byte[] toCompactByteArray();

  /**
   * Returns a compact binary representation of this account identifier as a {@link ByteString}.
   *
   * @return a binary representation of this account identifier
   */
  default ByteString toCompactByteString() {
    return ByteString.copyFrom(toCompactByteArray());
  }

  /**
   * Returns a fixed-width binary representation of this account identifier.
   *
   * @return a binary representation of this account identifier
   */
  byte[] toFixedWidthByteArray();

  /**
   * Returns a fixed-width binary representation of this account identifier as a {@link ByteString}.
   *
   * @return a binary representation of this account identifier
   */
  default ByteString toFixedWidthByteString() {
    return ByteString.copyFrom(toFixedWidthByteArray());
  }

  /**
   * Parse a service identifier string, which should be a plain UUID string for ACIs and a prefixed UUID string for PNIs
   *
   * @param string a service identifier string
   * @return the parsed service identifier
   */
  static ServiceIdentifier valueOf(final String string) {
    try {
      return AciServiceIdentifier.valueOf(string);
    } catch (final IllegalArgumentException e) {
      return PniServiceIdentifier.valueOf(string);
    }
  }

  /**
   * Parse a service identifier from a byte array.
   *
   * @param bytes the byte array from which to parse a service identifier
   * @return the parsed service identifier
   */
  static ServiceIdentifier fromBytes(final byte[] bytes) {
    try {
      return AciServiceIdentifier.fromBytes(bytes);
    } catch (final IllegalArgumentException e) {
      return PniServiceIdentifier.fromBytes(bytes);
    }
  }

  /**
   * Parse a service identifier from a byte string.
   *
   * @param byteString the byte string from which to parse a service identifier
   * @return the parsed service identifier
   */
  static ServiceIdentifier fromByteString(final ByteString byteString) {
    return fromBytes(byteString.toByteArray());
  }

  static ServiceIdentifier fromLibsignal(final ServiceId libsignalServiceId) {
    if (libsignalServiceId instanceof ServiceId.Aci) {
      return new AciServiceIdentifier(libsignalServiceId.getRawUUID());
    }
    if (libsignalServiceId instanceof ServiceId.Pni) {
      return new PniServiceIdentifier(libsignalServiceId.getRawUUID());
    }
    throw new IllegalArgumentException("unknown libsignal ServiceId type");
  }

  ServiceId toLibsignal();
}
