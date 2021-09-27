/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotEmpty;

public class BadgeConfiguration {
  public static final String CATEGORY_TESTING = "testing";

  private final String id;
  private final String category;
  private final String ldpi;
  private final String mdpi;
  private final String hdpi;
  private final String xhdpi;
  private final String xxhdpi;
  private final String xxxhdpi;
  private final String lowDetailSvg;
  private final String highDetailSvg;

  @JsonCreator
  public BadgeConfiguration(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("ldpi") final String ldpi,
      @JsonProperty("mdpi") final String mdpi,
      @JsonProperty("hdpi") final String hdpi,
      @JsonProperty("xhdpi") final String xhdpi,
      @JsonProperty("xxhdpi") final String xxhdpi,
      @JsonProperty("xxxhdpi") final String xxxhdpi,
      @JsonProperty("lowDetailSvg") final String lowDetailSvg,
      @JsonProperty("highDetailSvg") final String highDetailSvg) {
    this.id = id;
    this.category = category;
    this.ldpi = ldpi;
    this.mdpi = mdpi;
    this.hdpi = hdpi;
    this.xhdpi = xhdpi;
    this.xxhdpi = xxhdpi;
    this.xxxhdpi = xxxhdpi;
    this.lowDetailSvg = lowDetailSvg;
    this.highDetailSvg = highDetailSvg;
  }

  @NotEmpty
  public String getId() {
    return id;
  }

  @NotEmpty
  public String getCategory() {
    return category;
  }

  @NotEmpty
  public String getLdpi() {
    return ldpi;
  }

  @NotEmpty
  public String getMdpi() {
    return mdpi;
  }

  @NotEmpty
  public String getHdpi() {
    return hdpi;
  }

  @NotEmpty
  public String getXhdpi() {
    return xhdpi;
  }

  @NotEmpty
  public String getXxhdpi() {
    return xxhdpi;
  }

  @NotEmpty
  public String getXxxhdpi() {
    return xxxhdpi;
  }

  @NotEmpty
  public String getLowDetailSvg() {
    return lowDetailSvg;
  }

  @NotEmpty
  public String getHighDetailSvg() {
    return highDetailSvg;
  }

  public boolean isTestBadge() {
    return CATEGORY_TESTING.equals(category);
  }
}
