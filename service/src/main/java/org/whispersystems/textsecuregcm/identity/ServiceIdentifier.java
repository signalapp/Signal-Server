/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.identity;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.UUID;

/**
 * A "service identifier" is a tuple of a UUID and identity type that identifies an account and identity within the
 * Signal service.
 */
@Schema(
    type = "string",
    description = "A service identifier is a tuple of a UUID and identity type that identifies an account and identity within the Signal service.",
    subTypes = {AciServiceIdentifier.class, PniServiceIdentifier.class}
)
public interface ServiceIdentifier {

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
   * Returns a fixed-width binary representation of this account identifier.
   *
   * @return a binary representation of this account identifier
   */
  byte[] toFixedWidthByteArray();

  static ServiceIdentifier valueOf(final String string) {
    try {
      return AciServiceIdentifier.valueOf(string);
    } catch (final IllegalArgumentException e) {
      return PniServiceIdentifier.valueOf(string);
    }
  }

  static ServiceIdentifier fromBytes(final byte[] bytes) {
    try {
      return AciServiceIdentifier.fromBytes(bytes);
    } catch (final IllegalArgumentException e) {
      return PniServiceIdentifier.fromBytes(bytes);
    }
  }
}
