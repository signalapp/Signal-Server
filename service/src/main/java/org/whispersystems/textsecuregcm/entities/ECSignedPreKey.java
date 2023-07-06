/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ECPublicKeyAdapter;
import java.util.Arrays;
import java.util.Objects;

public record ECSignedPreKey(
    @Schema(description="""
        An arbitrary ID for this key, which will be provided by peers using this key to encrypt messages so the private key can be looked up.
        Should not be zero. Should be less than 2^24.
        """)
    long keyId,

    @JsonSerialize(using = ECPublicKeyAdapter.Serializer.class)
    @JsonDeserialize(using = ECPublicKeyAdapter.Deserializer.class)
    @Schema(type="string", description="""
        The public key, serialized in libsignal's elliptic-curve public key format and then base64-encoded.
        """)
    ECPublicKey publicKey,

    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @Schema(type="string", description="""
        The signature of the serialized `publicKey` with the account (or phone-number identity)'s identity key, base64-encoded.
        """)
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
