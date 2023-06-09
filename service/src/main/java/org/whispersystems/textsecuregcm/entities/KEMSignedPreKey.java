/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.KEMPublicKeyAdapter;
import java.util.Arrays;
import java.util.Objects;

public record KEMSignedPreKey(long keyId,

                              @JsonSerialize(using = KEMPublicKeyAdapter.Serializer.class)
                              @JsonDeserialize(using = KEMPublicKeyAdapter.Deserializer.class)
                              KEMPublicKey publicKey,

                              @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                              @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                              byte[] signature) implements SignedPreKey<KEMPublicKey> {

  @Override
  public byte[] serializedPublicKey() {
    return publicKey().serialize();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    KEMSignedPreKey that = (KEMSignedPreKey) o;
    return keyId == that.keyId && publicKey.equals(that.publicKey) && Arrays.equals(signature, that.signature);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(keyId, publicKey);
    result = 31 * result + Arrays.hashCode(signature);
    return result;
  }
}
