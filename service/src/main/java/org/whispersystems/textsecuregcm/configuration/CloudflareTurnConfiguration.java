/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import jakarta.validation.constraints.Positive;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

/**
 * Configuration properties for Cloudflare TURN integration.
 *
 * @param apiToken the API token to use when requesting TURN tokens from Cloudflare
 * @param endpoint the URI of the Cloudflare API endpoint that vends TURN tokens
 * @param requestedCredentialTtl the lifetime of TURN tokens to request from Cloudflare
 * @param clientCredentialTtl the time clients may cache a TURN token; must be less than or equal to {@link #requestedCredentialTtl}
 * @param urls a collection of TURN URLs to include verbatim in responses to clients
 * @param urlsWithIps a collection of {@link String#format(String, Object...)} patterns to be populated with resolved IP
 *                    addresses for {@link #hostname} in responses to clients; each pattern must include a single
 *                    {@code %s} placeholder for the IP address
 * @param circuitBreaker a circuit breaker for requests to Cloudflare
 * @param retry a retry policy for requests to Cloudflare
 * @param hostname the hostname to resolve to IP addresses for use with {@link #urlsWithIps}; also transmitted to
 *                 clients for use as an SNI when connecting to pre-resolved hosts
 * @param numHttpClients the number of parallel HTTP clients to use to communicate with Cloudflare
 */
public record CloudflareTurnConfiguration(@NotNull SecretString apiToken,
                                          @NotBlank String endpoint,
                                          @NotNull Duration requestedCredentialTtl,
                                          @NotNull Duration clientCredentialTtl,
                                          @NotNull @NotEmpty @Valid List<@NotBlank String> urls,
                                          @NotNull @NotEmpty @Valid List<@NotBlank String> urlsWithIps,
                                          @NotNull @Valid CircuitBreakerConfiguration circuitBreaker,
                                          @NotNull @Valid RetryConfiguration retry,
                                          @NotBlank String hostname,
                                          @Positive int numHttpClients) {

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

  @AssertTrue
  @Schema(hidden = true)
  public boolean isClientTtlShorterThanRequestedTtl() {
    return clientCredentialTtl.compareTo(requestedCredentialTtl) <= 0;
  }
}
