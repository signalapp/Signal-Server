/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ECPublicKeyAdapter;
import java.util.Arrays;
import java.util.Objects;

public record ECSignedPreKey(long keyId,

                             @JsonSerialize(using = ECPublicKeyAdapter.Serializer.class)
                             @JsonDeserialize(using = ECPublicKeyAdapter.Deserializer.class)
                             ECPublicKey publicKey,

                             @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                             @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                             byte[] signature) implements SignedPreKey<ECPublicKey> {

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
    ECSignedPreKey that = (ECSignedPreKey) o;
    return keyId == that.keyId && publicKey.equals(that.publicKey) && Arrays.equals(signature, that.signature);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(keyId, publicKey);
    result = 31 * result + Arrays.hashCode(signature);
    return result;
  }
}
