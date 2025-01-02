/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public class UserRemoteConfig {

  @JsonProperty
  @Schema(description = "Name of the configuration", example = "android.exampleFeature")
  private String name;

  @JsonProperty
  @Schema(description = "Whether the configuration is enabled for the user")
  private boolean enabled;

  @JsonProperty
  @Schema(description = "The value to be used for the configuration, if it is a non-boolean type")
  private String value;

  public UserRemoteConfig() {
  }

  public UserRemoteConfig(String name, boolean enabled, String value) {
    this.name = name;
    this.enabled = enabled;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String getValue() {
    return value;
  }
}
