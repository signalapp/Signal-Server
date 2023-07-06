/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.util.ECPublicKeyAdapter;

public record ECPreKey(
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
    ECPublicKey publicKey) implements PreKey<ECPublicKey> {

  @Override
  public byte[] serializedPublicKey() {
    return publicKey().serialize();
  }
}
