/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Objects;

public class Badge {
  private final String id;
  private final String category;
  private final String name;
  private final String description;
  private final List<String> sprites6;
  private final String svg;
  private final List<BadgeSvg> svgs;

  @JsonCreator
  public Badge(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("sprites6") final List<String> sprites6,
      @JsonProperty("svg") final String svg,
      @JsonProperty("svgs") final List<BadgeSvg> svgs) {
    this.id = id;
    this.category = category;
    this.name = name;
    this.description = description;
    this.sprites6 = Objects.requireNonNull(sprites6);
    if (sprites6.size() != 6) {
      throw new IllegalArgumentException("sprites must have size 6");
    }
    if (Strings.isNullOrEmpty(svg)) {
      throw new IllegalArgumentException("svg cannot be empty");
    }
    this.svg = svg;
    this.svgs = Objects.requireNonNull(svgs);
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

  public String getSvg() {
    return svg;
  }

  public List<BadgeSvg> getSvgs() {
    return svgs;
  }

  /**
   * Workaround for old Android builds that expect this field to exist but don't care it's an empty string.
   */
  @Deprecated
  @JsonProperty
  public String getImageUrl() {
    return "";
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
        && Objects.equals(svg, badge.svg)
        && Objects.equals(svgs, badge.svgs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, category, name, description, sprites6, svg, svgs);
  }

  @Override
  public String toString() {
    return "Badge{" +
        "id='" + id + '\'' +
        ", category='" + category + '\'' +
        ", name='" + name + '\'' +
        ", description='" + description + '\'' +
        ", sprites6=" + sprites6 +
        ", svg='" + svg + '\'' +
        ", svgs=" + svgs +
        '}';
  }
}
