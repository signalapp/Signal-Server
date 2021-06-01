/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotBlank;

public class DatadogConfiguration {

  @JsonProperty
  @NotBlank
  private String apiKey;

  public String getApiKey() {
    return apiKey;
  }
}
