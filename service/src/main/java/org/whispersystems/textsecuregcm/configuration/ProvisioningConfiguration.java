/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;


import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public record ProvisioningConfiguration(@Valid @NotNull SingletonRedisClientFactory pubsub,
                                        @Valid @NotNull CircuitBreakerConfiguration circuitBreaker) {

  public ProvisioningConfiguration {
    if (circuitBreaker == null) {
      circuitBreaker = new CircuitBreakerConfiguration();
    }
  }
}
