/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;

public class AccountBadge {

  private final String id;
  private final Instant expiration;
  private final boolean visible;

  @JsonCreator
  public AccountBadge(
      @JsonProperty("id") String id,
      @JsonProperty("expiration") Instant expiration,
      @JsonProperty("visible") boolean visible) {
    this.id = id;
    this.expiration = expiration;
    this.visible = visible;
  }

  public String getId() {
    return id;
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
    AccountBadge that = (AccountBadge) o;
    return visible == that.visible && Objects.equals(id, that.id)
        && Objects.equals(expiration, that.expiration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, expiration, visible);
  }

  @Override
  public String toString() {
    return "AccountBadge{" +
        "id='" + id + '\'' +
        ", expiration=" + expiration +
        ", visible=" + visible +
        '}';
  }
}
