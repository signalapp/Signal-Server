/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;

public class ActiveUserTally {
  @JsonProperty
  private UUID fromUuid;

  @JsonProperty
  private Map<String, long[]> platforms;

  @JsonProperty
  private Map<String, long[]> countries;

  public ActiveUserTally() {}

  public ActiveUserTally(UUID fromUuid, Map<String, long[]> platforms, Map<String, long[]> countries) {
    this.fromUuid   = fromUuid;
    this.platforms  = platforms;
    this.countries  = countries;
  }

  public UUID getFromUuid() {
    return this.fromUuid;
  }

  public Map<String, long[]> getPlatforms() {
    return this.platforms;
  }

  public Map<String, long[]> getCountries() {
    return this.countries;
  }

  public void setFromUuid(UUID fromUuid) {
    this.fromUuid = fromUuid;
  }

}
