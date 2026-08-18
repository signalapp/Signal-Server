/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.Arrays;
import java.util.Objects;
import javax.crypto.SecretKey;

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

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof TotpKey(TotpParameters parameters, byte[] key))) {
      return false;
    }

    return Objects.deepEquals(encodedKey, key) && Objects.equals(totpParameters, parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(totpParameters, Arrays.hashCode(encodedKey));
  }
}
