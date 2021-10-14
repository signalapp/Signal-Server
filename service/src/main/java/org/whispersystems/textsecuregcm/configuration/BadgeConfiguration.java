/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class BadgeConfiguration {
  public static final String CATEGORY_TESTING = "testing";

  private final String id;
  private final String category;
  private final List<String> sprites;
  private final List<String> svgs;

  @JsonCreator
  public BadgeConfiguration(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("sprites") final List<String> sprites,
      @JsonProperty("svgs") final List<String> svgs) {
    this.id = id;
    this.category = category;
    this.sprites = sprites;
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

  @NotNull
  @ExactlySize(4)
  public List<String> getSvgs() {
    return svgs;
  }

  public boolean isTestBadge() {
    return CATEGORY_TESTING.equals(category);
  }
}
