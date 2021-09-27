/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class Badge {
  private final String id;
  private final String category;
  private final String name;
  private final String description;
  private final String ldpi;
  private final String mdpi;
  private final String hdpi;
  private final String xhdpi;
  private final String xxhdpi;
  private final String xxxhdpi;

  @JsonCreator
  public Badge(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("ldpi") final String ldpi,
      @JsonProperty("mdpi") final String mdpi,
      @JsonProperty("hdpi") final String hdpi,
      @JsonProperty("xhdpi") final String xhdpi,
      @JsonProperty("xxhdpi") final String xxhdpi,
      @JsonProperty("xxxhdpi") final String xxxhdpi) {
    this.id = id;
    this.category = category;
    this.name = name;
    this.description = description;
    this.ldpi = ldpi;
    this.mdpi = mdpi;
    this.hdpi = hdpi;
    this.xhdpi = xhdpi;
    this.xxhdpi = xxhdpi;
    this.xxxhdpi = xxxhdpi;
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

  public String getLdpi() {
    return ldpi;
  }

  public String getMdpi() {
    return mdpi;
  }

  public String getHdpi() {
    return hdpi;
  }

  public String getXhdpi() {
    return xhdpi;
  }

  public String getXxhdpi() {
    return xxhdpi;
  }

  public String getXxxhdpi() {
    return xxxhdpi;
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
        badge.category) && Objects.equals(name, badge.name) && Objects.equals(
        description, badge.description) && Objects.equals(ldpi, badge.ldpi)
        && Objects.equals(mdpi, badge.mdpi) && Objects.equals(hdpi, badge.hdpi)
        && Objects.equals(xhdpi, badge.xhdpi) && Objects.equals(xxhdpi,
        badge.xxhdpi) && Objects.equals(xxxhdpi, badge.xxxhdpi);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, category, name, description, ldpi, mdpi, hdpi, xhdpi, xxhdpi, xxxhdpi);
  }
}
