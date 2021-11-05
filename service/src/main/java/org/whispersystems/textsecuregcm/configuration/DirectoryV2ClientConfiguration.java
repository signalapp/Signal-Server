/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class DirectoryV2ClientConfiguration {

  private final byte[] userAuthenticationTokenSharedSecret;

  @JsonCreator
  public DirectoryV2ClientConfiguration(
      @JsonProperty("userAuthenticationTokenSharedSecret") final byte[] userAuthenticationTokenSharedSecret) {
    this.userAuthenticationTokenSharedSecret = userAuthenticationTokenSharedSecret;
  }

  @ExactlySize({32})
  public byte[] getUserAuthenticationTokenSharedSecret() {
    return userAuthenticationTokenSharedSecret;
  }

}
