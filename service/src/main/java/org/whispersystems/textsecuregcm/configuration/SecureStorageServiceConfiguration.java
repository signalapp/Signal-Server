/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public record SecureStorageServiceConfiguration(@NotEmpty String userAuthenticationTokenSharedSecret,
                                                @NotBlank String uri,
                                                @NotEmpty List<@NotBlank String> storageCaCertificates,
                                                @Valid @JsonProperty("circuitBreaker") CircuitBreakerConfiguration circuitBreakerConfig,
                                                @Valid @JsonProperty("retry") RetryConfiguration retryConfig) {

  @VisibleForTesting
  public SecureStorageServiceConfiguration(
      final @NotEmpty String userAuthenticationTokenSharedSecret,
      final @NotBlank String uri,
      final @NotEmpty List<@NotBlank String> storageCaCertificates) {
    this(userAuthenticationTokenSharedSecret, uri, storageCaCertificates, new CircuitBreakerConfiguration(), new RetryConfiguration());
  }

  public byte[] decodeUserAuthenticationTokenSharedSecret() throws DecoderException {
    return Hex.decodeHex(userAuthenticationTokenSharedSecret.toCharArray());
  }
}
