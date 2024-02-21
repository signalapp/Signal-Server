/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.kem.KEMPublicKey;

public class KEMPublicKeyAdapter {

  public static class Serializer extends AbstractPublicKeySerializer<KEMPublicKey> {

    @Override
    protected byte[] serializePublicKey(final KEMPublicKey publicKey) {
      return publicKey.serialize();
    }
  }

  public static class Deserializer extends AbstractPublicKeyDeserializer<KEMPublicKey> {

    @Override
    protected KEMPublicKey deserializePublicKey(final byte[] publicKeyBytes) throws InvalidKeyException {
      return new KEMPublicKey(publicKeyBytes);
    }
  }
}
