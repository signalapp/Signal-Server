/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

public class ECPublicKeyAdapter {

  public static class Serializer extends AbstractPublicKeySerializer<ECPublicKey> {

    @Override
    protected byte[] serializePublicKey(final ECPublicKey publicKey) {
      return publicKey.serialize();
    }
  }

  public static class Deserializer extends AbstractPublicKeyDeserializer<ECPublicKey> {

    @Override
    protected ECPublicKey deserializePublicKey(final byte[] publicKeyBytes) throws InvalidKeyException {
      return new ECPublicKey(publicKeyBytes);
    }
  }
}
