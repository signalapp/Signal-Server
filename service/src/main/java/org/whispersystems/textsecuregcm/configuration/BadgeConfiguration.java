/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotEmpty;

public class BadgeConfiguration {
  private final String name;
  private final String imageUrl;

  @JsonCreator
  public BadgeConfiguration(
      @JsonProperty("name") final String name,
      @JsonProperty("imageUrl") final String imageUrl) {
    this.name = name;
    this.imageUrl = imageUrl;
  }

  @NotEmpty
  public String getName() {
    return name;
  }

  @NotEmpty
  public String getImageUrl() {
    return imageUrl;
  }
}
