/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class DynamicTurnConfiguration {

  @JsonProperty
  @NotNull
  @Valid
  private List<@NotBlank String> urls = Collections.emptyList();

  @JsonProperty
  @NotNull
  @Valid
  private List<@NotBlank String> urlsWithIps = Collections.emptyList();

  @JsonProperty
  @Nullable
  private String hostname = null;

  public DynamicTurnConfiguration() {}

  public List<String> getUrls() {
    return urls;
  }

  public List<String> getUrlsWithIps() {
    return urlsWithIps;
  }

  public @Nullable String getHostname() {
    return hostname;
  }
}
