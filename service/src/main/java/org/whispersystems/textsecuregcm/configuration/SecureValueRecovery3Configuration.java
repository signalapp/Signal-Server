/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.ExactlySize;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;

public record SecureValueRecovery3Configuration(
    @NotBlank String uri,
    @ExactlySize(32) SecretBytes userAuthenticationTokenSharedSecret,
    @ExactlySize(32) SecretBytes userIdTokenSharedSecret,
    @NotEmpty List<@NotBlank String> svrCaCertificates,
    @NotNull @Valid CircuitBreakerConfiguration circuitBreaker,
    @NotNull @Valid RetryConfiguration retry) {

  public SecureValueRecovery3Configuration {
    if (circuitBreaker == null) {
      circuitBreaker = new CircuitBreakerConfiguration();
    }

    if (retry == null) {
      retry = new RetryConfiguration();
    }
  }
}
