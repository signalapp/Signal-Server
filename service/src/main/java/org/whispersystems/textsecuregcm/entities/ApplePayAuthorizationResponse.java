/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Strings;
import javax.validation.constraints.NotEmpty;

public class ApplePayAuthorizationResponse {

  private final String id;
  private final String clientSecret;

  @JsonCreator
  public ApplePayAuthorizationResponse(
      @JsonProperty("id") final String id,
      @JsonProperty("client_secret") final String clientSecret) {
    if (Strings.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("id cannot be empty");
    }
    if (Strings.isNullOrEmpty(clientSecret)) {
      throw new IllegalArgumentException("clientSecret cannot be empty");
    }

    this.id = id;
    this.clientSecret = clientSecret;
  }

  @JsonProperty("id")
  @NotEmpty
  public String getId() {
    return id;
  }

  @JsonProperty("client_secret")
  @NotEmpty
  public String getClientSecret() {
    return clientSecret;
  }
}
