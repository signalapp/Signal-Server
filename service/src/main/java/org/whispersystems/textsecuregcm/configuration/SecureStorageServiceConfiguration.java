/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import java.util.List;

public class SecureStorageServiceConfiguration {

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenSharedSecret;

  @NotBlank
  @JsonProperty
  private String uri;

  @NotEmpty
  @JsonProperty
  private List<@NotBlank String> storageCaCertificates;

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

  @VisibleForTesting
  public void setStorageCaCertificates(final List<String> certificatePem) {
    this.storageCaCertificates = certificatePem;
  }

  public List<String> getStorageCaCertificates() {
    return storageCaCertificates;
  }

  public CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
    return circuitBreaker;
  }

  public RetryConfiguration getRetryConfiguration() {
    return retry;
  }
}
