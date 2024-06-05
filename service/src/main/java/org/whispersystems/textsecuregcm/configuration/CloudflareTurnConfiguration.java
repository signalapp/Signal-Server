/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public record CloudflareTurnConfiguration(@NotNull SecretString apiToken,
                                          @NotBlank String endpoint,
                                          @NotBlank long ttl,
                                          @NotBlank List<String> urls,
                                          @NotBlank List<String> urlsWithIps,
                                          @NotNull @Valid CircuitBreakerConfiguration circuitBreaker,
                                          @NotNull @Valid RetryConfiguration retry,
                                          @NotBlank String hostname) {

  public CloudflareTurnConfiguration {
    if (circuitBreaker == null) {
      // It’s a little counter-intuitive, but this compact constructor allows a default value
      // to be used when one isn’t specified (e.g. in YAML), allowing the field to still be
      // validated as @NotNull
      circuitBreaker = new CircuitBreakerConfiguration();
    }

    if (retry == null) {
      retry = new RetryConfiguration();
    }
  }
}
