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

  /**
   * Returns a new AccountBadge that is a merging of the two originals. IDs must match for this operation to make sense.
   * The expiration will be the later of the two.
   * Visibility will be set if either of the passed in objects is visible.
   */
  public AccountBadge mergeWith(AccountBadge other) {
    if (!Objects.equals(other.id, id)) {
      throw new IllegalArgumentException("merging badges should only take place for same id");
    }

    final Instant latestExpiration;
    if (expiration == null || other.expiration == null) {
      latestExpiration = null;
    } else if (expiration.isAfter(other.expiration)) {
      latestExpiration = expiration;
    } else {
      latestExpiration = other.expiration;
    }

    return new AccountBadge(
        id,
        latestExpiration,
        visible || other.visible
    );
  }

  public AccountBadge withVisibility(boolean visible) {
    if (this.visible == visible) {
      return this;
    } else {
      return new AccountBadge(
          this.id,
          this.expiration,
          visible);
    }
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
