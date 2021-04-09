/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserRemoteConfig {

  @JsonProperty
  private String  name;

  @JsonProperty
  private boolean enabled;

  @JsonProperty
  private String  value;

  public UserRemoteConfig() {}

  public UserRemoteConfig(String name, boolean enabled, String value) {
    this.name    = name;
    this.enabled = enabled;
    this.value   = value;
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
