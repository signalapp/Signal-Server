/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.HexFormat;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

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

  public byte[] decodeUserAuthenticationTokenSharedSecret() {
    return HexFormat.of().parseHex(userAuthenticationTokenSharedSecret);
  }
}
