/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import javax.crypto.SecretKey;

public record AnnotatedTotpKey(@JsonUnwrapped
                               TotpKey totpKey,

                               @JsonProperty("metadata")
                               byte[] metadataCiphertext) implements SecretKey {

  @Override
  public String getAlgorithm() {
    return totpKey().getAlgorithm();
  }

  @Override
  public String getFormat() {
    return totpKey.getFormat();
  }

  @Override
  public byte[] getEncoded() {
    return totpKey().getEncoded();
  }
}
