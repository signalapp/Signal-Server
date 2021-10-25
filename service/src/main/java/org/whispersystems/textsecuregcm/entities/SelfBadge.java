/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Extension of the Badge object returned when asking for one's own badges.
 */
public class SelfBadge extends Badge {
  private final Instant expiration;
  private final boolean visible;

  public SelfBadge(
      @JsonProperty("id") final String id,
      @JsonProperty("category") final String category,
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("sprites6") final List<String> sprites6,
      @JsonProperty("svg") final String svg,
      @JsonProperty("svgs") final List<BadgeSvg> svgs,
      @JsonProperty("expiration") final Instant expiration,
      @JsonProperty("visible") final boolean visible) {
    super(id, category, name, description, sprites6, svg, svgs);
    this.expiration = expiration;
    this.visible = visible;
  }

  public Instant getExpiration() {
    return expiration;
  }

  public boolean isVisible() {
    return visible;
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
    SelfBadge selfBadge = (SelfBadge) o;
    return visible == selfBadge.visible && Objects.equals(expiration, selfBadge.expiration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), expiration, visible);
  }

  @Override
  public String toString() {
    return "SelfBadge{" +
        "expiration=" + expiration +
        ", visible=" + visible +
        '}';
  }
}
