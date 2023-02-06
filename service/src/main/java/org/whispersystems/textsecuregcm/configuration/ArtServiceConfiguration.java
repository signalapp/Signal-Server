/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.HexFormat;
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

  public byte[] getUserAuthenticationTokenSharedSecret() {
    return HexFormat.of().parseHex(userAuthenticationTokenSharedSecret);
  }

  public byte[] getUserAuthenticationTokenUserIdSecret() {
    return HexFormat.of().parseHex(userAuthenticationTokenUserIdSecret);
  }

  public Duration getTokenExpiration() {
    return tokenExpiration;
  }
}
