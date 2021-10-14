/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

public class Badge {
  private final String id;
  private final String category;
  private final String name;
  private final String description;
  private final List<String> sprites6;
  private final List<String> svgs4;

  @JsonCreator
  public Badge(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("sprites6") final List<String> sprites6,
      @JsonProperty("svgs4") final List<String> svgs4) {
    this.id = id;
    this.category = category;
    this.name = name;
    this.description = description;
    this.sprites6 = Objects.requireNonNull(sprites6);
    if (sprites6.size() != 6) {
      throw new IllegalArgumentException("sprites must have size 6");
    }
    this.svgs4 = Objects.requireNonNull(svgs4);
    if (svgs4.size() != 4) {
      throw new IllegalArgumentException("svgs must have size 4");
    }
  }

  public String getId() {
    return id;
  }

  public String getCategory() {
    return category;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getSprites6() {
    return sprites6;
  }

  public List<String> getSvgs4() {
    return svgs4;
  }

  @Deprecated
  public String getLdpi() {
    return sprites6.get(0);
  }

  @Deprecated
  public String getMdpi() {
    return sprites6.get(1);
  }

  @Deprecated
  public String getHdpi() {
    return sprites6.get(2);
  }

  @Deprecated
  public String getXhdpi() {
    return sprites6.get(3);
  }

  @Deprecated
  public String getXxhdpi() {
    return sprites6.get(4);
  }

  @Deprecated
  public String getXxxhdpi() {
    return sprites6.get(5);
  }

  @Deprecated
  public String getLsvg() {
    return svgs4.get(0);
  }

  @Deprecated
  public String getHsvg() {
    return svgs4.get(3);
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
    return Objects.equals(id, badge.id)
        && Objects.equals(category, badge.category)
        && Objects.equals(name, badge.name)
        && Objects.equals(description, badge.description)
        && Objects.equals(sprites6, badge.sprites6)
        && Objects.equals(svgs4, badge.svgs4);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, category, name, description, sprites6, svgs4);
  }
}
