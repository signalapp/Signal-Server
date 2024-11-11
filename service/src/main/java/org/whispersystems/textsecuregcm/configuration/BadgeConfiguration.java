/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class BadgeConfiguration {
  public static final String CATEGORY_TESTING = "testing";

  private final String id;
  private final String category;
  private final List<String> sprites;
  private final String svg;
  private final List<BadgeSvg> svgs;

  @JsonCreator
  public BadgeConfiguration(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("sprites") final List<String> sprites,
      @JsonProperty("svg") final String svg,
      @JsonProperty("svgs") final List<BadgeSvg> svgs) {
    this.id = id;
    this.category = category;
    this.sprites = sprites;
    this.svg = svg;
    this.svgs = svgs;
  }

  @NotEmpty
  public String getId() {
    return id;
  }

  @NotEmpty
  public String getCategory() {
    return category;
  }

  @NotNull
  @ExactlySize(6)
  public List<String> getSprites() {
    return sprites;
  }

  @NotEmpty
  public String getSvg() {
    return svg;
  }

  @NotNull
  public List<BadgeSvg> getSvgs() {
    return svgs;
  }

  public boolean isTestBadge() {
    return CATEGORY_TESTING.equals(category);
  }
}
