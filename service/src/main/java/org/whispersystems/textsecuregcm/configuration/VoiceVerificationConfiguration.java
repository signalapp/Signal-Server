/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class VoiceVerificationConfiguration {

  @JsonProperty
  @Valid
  @NotEmpty
  private String url;

  @JsonProperty
  @Valid
  @NotNull
  private List<String> locales;

  public String getUrl() {
    return url;
  }

  public Set<String> getLocales() {
    return new HashSet<>(locales);
  }
}
