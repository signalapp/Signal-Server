/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.Map;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

/**
 * @param merchantId          the Braintree merchant ID
 * @param publicKey           the Braintree API public key
 * @param privateKey          the Braintree API private key
 * @param environment         the Braintree environment ("production" or "sandbox")
 * @param supportedCurrencies the set of supported currencies
 * @param graphqlUrl          the Braintree GraphQL URl to use (this must match the environment)
 * @param merchantAccounts    merchant account within the merchant for processing individual currencies
 * @param circuitBreaker      configuration for the circuit breaker used by the GraphQL HTTP client
 */
public record BraintreeConfiguration(@NotBlank String merchantId,
                                     @NotBlank String publicKey,
                                     @NotBlank String privateKey,
                                     @NotBlank String environment,
                                     @NotEmpty Set<@NotBlank String> supportedCurrencies,
                                     @NotBlank String graphqlUrl,
                                     @NotEmpty Map<String, String> merchantAccounts,
                                     @NotNull
                                     @Valid
                                     CircuitBreakerConfiguration circuitBreaker) {

  public BraintreeConfiguration {
    if (circuitBreaker == null) {
      // It’s a little counter-intuitive, but this compact constructor allows a default value
      // to be used when one isn’t specified (e.g. in YAML), allowing the field to still be
      // validated as @NotNull
      circuitBreaker = new CircuitBreakerConfiguration();
    }
  }
}
