/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record SecureValueRecovery2Configuration(
    @ExactlySize({32}) byte[] userAuthenticationTokenSharedSecret,
    @ExactlySize({32}) byte[] userIdTokenSharedSecret,
    @NotBlank String uri,
    @NotEmpty List<@NotBlank String> svrCaCertificates,
    @NotNull @Valid CircuitBreakerConfiguration circuitBreaker,
    @NotNull @Valid RetryConfiguration retry) {

  public SecureValueRecovery2Configuration {
    if (circuitBreaker == null) {
      circuitBreaker = new CircuitBreakerConfiguration();
    }

    if (retry == null) {
      retry = new RetryConfiguration();
    }
  }
}
