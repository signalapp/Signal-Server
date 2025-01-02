/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.List;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.util.InstantAdapter;

public class UserRemoteConfigList {

  @JsonProperty
  @Schema(description = "List of remote configurations applicable to the user")
  private List<UserRemoteConfig> config;

  @JsonProperty
  @JsonSerialize(using = InstantAdapter.EpochSecondSerializer.class)
  @JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
  @Schema(description = """
    Timestamp when the configuration was generated. Deprecated in favor of `X-Signal-Timestamp` response header.
    """, deprecated = true)
  @Deprecated
  private Instant serverEpochTime;

  public UserRemoteConfigList() {}

  public UserRemoteConfigList(List<UserRemoteConfig> config, Instant serverEpochTime) {
    this.config = config;
    this.serverEpochTime = serverEpochTime;
  }

  public List<UserRemoteConfig> getConfig() {
    return config;
  }

  public Instant getServerEpochTime() {
    return serverEpochTime;
  }
}
