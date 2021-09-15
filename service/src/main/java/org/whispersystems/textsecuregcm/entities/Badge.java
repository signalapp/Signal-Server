/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;
import java.util.Objects;

public class Badge {
  private final String id;
  private final String category;
  private final URL imageUrl;
  private final String name;
  private final String description;

  @JsonCreator
  public Badge(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("imageUrl") final URL imageUrl,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description) {
    this.id = id;
    this.category = category;
    this.imageUrl = imageUrl;
    this.name = name;
    this.description = description;
  }

  public String getId() {
    return id;
  }

  public String getCategory() {
    return category;
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Badge badge = (Badge) o;
    return Objects.equals(id, badge.id) && Objects.equals(category,
        badge.category) && Objects.equals(imageUrl, badge.imageUrl)
        && Objects.equals(name, badge.name) && Objects.equals(description,
        badge.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, category, imageUrl, name, description);
  }
}
