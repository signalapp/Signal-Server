/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.util.ECPublicKeyAdapter;

public record ECPreKey(long keyId,
                       @JsonSerialize(using = ECPublicKeyAdapter.Serializer.class)
                       @JsonDeserialize(using = ECPublicKeyAdapter.Deserializer.class)
                       ECPublicKey publicKey) implements PreKey<ECPublicKey> {

  @Override
  public byte[] serializedPublicKey() {
    return publicKey().serialize();
  }
}
