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

  private final String name;
  private final Instant expiration;

  @JsonCreator
  public AccountBadge(
      @JsonProperty("name") String name,
      @JsonProperty("expiration") Instant expiration) {
    this.name = name;
    this.expiration = expiration;
  }

  public String getName() {
    return name;
  }

  public Instant getExpiration() {
    return expiration;
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
    return Objects.equals(name, that.name)
        && Objects.equals(expiration, that.expiration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, expiration);
  }

  @Override
  public String toString() {
    return "AccountBadge{" +
        "name='" + name + '\'' +
        ", expiration=" + expiration +
        '}';
  }
}
