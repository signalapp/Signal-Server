/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.time.Duration;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class ArtServiceConfiguration {

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenSharedSecret;

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenUserIdSecret;

  @JsonProperty
  @NotNull
  private Duration tokenExpiration = Duration.ofDays(1);

  public byte[] getUserAuthenticationTokenSharedSecret() throws DecoderException {
    return Hex.decodeHex(userAuthenticationTokenSharedSecret.toCharArray());
  }

  public byte[] getUserAuthenticationTokenUserIdSecret() throws DecoderException {
    return Hex.decodeHex(userAuthenticationTokenUserIdSecret.toCharArray());
  }

  public Duration getTokenExpiration() {
    return tokenExpiration;
  }
}
