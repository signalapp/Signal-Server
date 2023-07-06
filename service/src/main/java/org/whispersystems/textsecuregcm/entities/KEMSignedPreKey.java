/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.KEMPublicKeyAdapter;
import java.util.Arrays;
import java.util.Objects;

public record KEMSignedPreKey(
    @Schema(description="""
        An arbitrary ID for this key, which will be provided by peers using this key to encrypt messages so the private key can be looked up.
        Should not be zero. Should be less than 2^24. The owner of this key must be able to determine from the key ID whether this represents
        a single-use or last-resort key, but another party should *not* be able to tell.
        """)
    long keyId,

    @JsonSerialize(using = KEMPublicKeyAdapter.Serializer.class)
    @JsonDeserialize(using = KEMPublicKeyAdapter.Deserializer.class)
    @Schema(type="string", description="""
        The public key, serialized in libsignal's Kyber1024 public key format and then base64-encoded.
        """)
    KEMPublicKey publicKey,

    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @Schema(type="string", description="""
        The signature of the serialized `publicKey` with the account (or phone-number identity)'s identity key, base64-encoded.
        """)
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
