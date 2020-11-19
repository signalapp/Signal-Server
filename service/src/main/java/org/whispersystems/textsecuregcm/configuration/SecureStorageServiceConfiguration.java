/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public class SecureStorageServiceConfiguration {

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenSharedSecret;

  @NotBlank
  @JsonProperty
  private String uri;

  @NotNull
  @Valid
  @JsonProperty
  private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @NotNull
  @Valid
  @JsonProperty
  private RetryConfiguration retry = new RetryConfiguration();

  public byte[] getUserAuthenticationTokenSharedSecret() throws DecoderException {
    return Hex.decodeHex(userAuthenticationTokenSharedSecret.toCharArray());
  }

  @VisibleForTesting
  public void setUri(final String uri) {
    this.uri = uri;
  }

  public String getUri() {
    return uri;
  }

  public CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
    return circuitBreaker;
  }

  public RetryConfiguration getRetryConfiguration() {
    return retry;
  }
}
