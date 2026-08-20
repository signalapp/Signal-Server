/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import javax.annotation.Nullable;
import javax.crypto.SecretKey;
import java.util.Arrays;

public record TotpKey(@JsonUnwrapped
                      TotpParameters totpParameters,

                      @JsonProperty("key")
                      byte[] encodedKey) implements SecretKey {

  @Override
  public String getAlgorithm() {
    return totpParameters().algorithm();
  }

  @Override
  public String getFormat() {
    return "RAW";
  }

  @Override
  public byte[] getEncoded() {
    return Arrays.copyOf(encodedKey(), encodedKey().length);
  }
}
