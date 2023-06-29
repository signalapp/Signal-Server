/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

public class PurchasableBadge extends Badge {
  private final Duration duration;

  @JsonCreator
  public PurchasableBadge(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("sprites6") final List<String> sprites6,
      @JsonProperty("svg") final String svg,
      @JsonProperty("svgs") final List<BadgeSvg> svgs,
      @JsonProperty("duration") final Duration duration) {
    super(id, category, name, description, sprites6, svg, svgs);
    this.duration = duration != null ? duration.truncatedTo(ChronoUnit.SECONDS) : null;
  }

  public PurchasableBadge(final Badge badge, final Duration duration) {
    super(
        badge.getId(),
        badge.getCategory(),
        badge.getName(),
        badge.getDescription(),
        badge.getSprites6(),
        badge.getSvg(),
        badge.getSvgs());
    this.duration = duration != null ? duration.truncatedTo(ChronoUnit.SECONDS) : null;
  }

  @JsonFormat(shape = Shape.NUMBER_INT)
  public Duration getDuration() {
    return duration;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    PurchasableBadge that = (PurchasableBadge) o;
    return Objects.equals(duration, that.duration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), duration);
  }

  @Override
  public String toString() {
    return "PurchasableBadge{" +
        "super=" + super.toString() +
        ", duration=" + duration +
        '}';
  }
}
