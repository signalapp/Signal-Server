/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;


public class ApnConfiguration {

  @NotEmpty
  @JsonProperty
  private String teamId;

  @NotEmpty
  @JsonProperty
  private String keyId;

  @NotEmpty
  @JsonProperty
  private String signingKey;

  @NotEmpty
  @JsonProperty
  private String bundleId;

  @JsonProperty
  private boolean sandbox = false;

  public String getTeamId() {
    return teamId;
  }

  public String getKeyId() {
    return keyId;
  }

  public String getSigningKey() {
    return signingKey;
  }

  public String getBundleId() {
    return bundleId;
  }

  public boolean isSandboxEnabled() {
    return sandbox;
  }
}
