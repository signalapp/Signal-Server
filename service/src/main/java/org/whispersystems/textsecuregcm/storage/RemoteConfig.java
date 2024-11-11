/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class RemoteConfig {

  @JsonProperty
  @Pattern(regexp = "[A-Za-z0-9\\.]+")
  private String name;

  @JsonProperty
  @NotNull
  @Min(0)
  @Max(100)
  private int percentage;

  @JsonProperty
  @NotNull
  private Set<UUID> uuids = new HashSet<>();

  @JsonProperty
  private String defaultValue;

  @JsonProperty
  private String value;

  @JsonProperty
  private String hashKey;

  public RemoteConfig() {}

  public RemoteConfig(String name, int percentage, Set<UUID> uuids, String defaultValue, String value, String hashKey) {
    this.name         = name;
    this.percentage   = percentage;
    this.uuids        = uuids;
    this.defaultValue = defaultValue;
    this.value        = value;
    this.hashKey      = hashKey;
  }

  public int getPercentage() {
    return percentage;
  }

  public String getName() {
    return name;
  }

  public Set<UUID> getUuids() {
    return uuids;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String getValue() {
    return value;
  }

  public String getHashKey() {
    return hashKey;
  }
}
