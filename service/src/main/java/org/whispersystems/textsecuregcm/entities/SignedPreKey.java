/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.signal.libsignal.protocol.IdentityKey;

public interface SignedPreKey<K> extends PreKey<K> {

  byte[] signature();

  default boolean signatureValid(final IdentityKey identityKey) {
    try {
      return identityKey.getPublicKey().verifySignature(serializedPublicKey(), signature());
    } catch (final Exception e) {
      return false;
    }
  }
}
