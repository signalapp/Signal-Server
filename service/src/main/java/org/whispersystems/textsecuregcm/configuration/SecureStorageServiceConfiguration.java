/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public record SecureStorageServiceConfiguration(@NotEmpty String userAuthenticationTokenSharedSecret,
                                                @NotBlank String uri,
                                                @NotEmpty List<@NotBlank String> storageCaCertificates,
                                                @Valid CircuitBreakerConfiguration circuitBreaker,
                                                @Valid RetryConfiguration retry) {

  public SecureStorageServiceConfiguration {
    if (circuitBreaker == null) {
      circuitBreaker = new CircuitBreakerConfiguration();
    }
    if (retry == null) {
      retry = new RetryConfiguration();
    }
  }

  public byte[] decodeUserAuthenticationTokenSharedSecret() throws DecoderException {
    return Hex.decodeHex(userAuthenticationTokenSharedSecret.toCharArray());
  }
}
