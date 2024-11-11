/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class SpamFilterConfiguration {

  @JsonProperty
  @NotBlank
  private final String environment;

  @JsonCreator
  public SpamFilterConfiguration(@JsonProperty("environment") final String environment) {
    this.environment = environment;
  }

  public String getEnvironment() {
    return environment;
  }
}
