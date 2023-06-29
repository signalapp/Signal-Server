/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class UserRemoteConfigList {

  @JsonProperty
  private List<UserRemoteConfig> config;

  @JsonProperty
  @JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
  private Instant serverEpochTime;

  public UserRemoteConfigList() {}

  public UserRemoteConfigList(List<UserRemoteConfig> config, Instant serverEpochTime) {
    this.config = config;
    this.serverEpochTime = serverEpochTime != null ? serverEpochTime.truncatedTo(ChronoUnit.SECONDS) : null;
  }

  public List<UserRemoteConfig> getConfig() {
    return config;
  }

  public Instant getServerEpochTime() {
    return serverEpochTime;
  }
}
