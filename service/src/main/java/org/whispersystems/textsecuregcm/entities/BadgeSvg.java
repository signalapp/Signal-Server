/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import java.util.Objects;
import javax.validation.constraints.NotEmpty;

public class BadgeSvg {
  private final String light;
  private final String dark;
  private final String transparent;

  @JsonCreator
  public BadgeSvg(
      @JsonProperty("light") final String light,
      @JsonProperty("dark") final String dark,
      @JsonProperty("transparent") final String transparent) {
    if (Strings.isNullOrEmpty(light)) {
      throw new IllegalArgumentException("light cannot be empty");
    }
    this.light = light;
    if (Strings.isNullOrEmpty(dark)) {
      throw new IllegalArgumentException("dark cannot be empty");
    }
    this.dark = dark;
    if (Strings.isNullOrEmpty(transparent)) {
      throw new IllegalArgumentException("transparent cannot be empty");
    }
    this.transparent = transparent;
  }

  @NotEmpty
  public String getLight() {
    return light;
  }

  @NotEmpty
  public String getDark() {
    return dark;
  }

  @NotEmpty
  public String getTransparent() {
    return transparent;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BadgeSvg badgeSvg = (BadgeSvg) o;
    return Objects.equals(light, badgeSvg.light)
        && Objects.equals(dark, badgeSvg.dark)
        && Objects.equals(transparent, badgeSvg.transparent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(light, dark, transparent);
  }

  @Override
  public String toString() {
    return "BadgeSvg{" +
        "light='" + light + '\'' +
        ", dark='" + dark + '\'' +
        ", transparent='" + transparent + '\'' +
        '}';
  }
}
