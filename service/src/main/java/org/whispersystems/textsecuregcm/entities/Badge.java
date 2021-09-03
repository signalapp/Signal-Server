/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;

public class Badge {
  private final URL imageUrl;
  private final String name;
  private final String description;

  @JsonCreator
  public Badge(
      @JsonProperty("imageUrl") final URL imageUrl,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description) {
    this.imageUrl = imageUrl;
    this.name = name;
    this.description = description;
  }

  public URL getImageUrl() {
    return imageUrl;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }
}
