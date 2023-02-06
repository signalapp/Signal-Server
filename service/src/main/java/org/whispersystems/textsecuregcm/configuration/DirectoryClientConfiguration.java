/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HexFormat;
import javax.validation.constraints.NotEmpty;

public class DirectoryClientConfiguration {

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenSharedSecret;

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenUserIdSecret;

  public byte[] getUserAuthenticationTokenSharedSecret() {
    return HexFormat.of().parseHex(userAuthenticationTokenSharedSecret);
  }

  public byte[] getUserAuthenticationTokenUserIdSecret() {
    return HexFormat.of().parseHex(userAuthenticationTokenUserIdSecret);
  }

}
