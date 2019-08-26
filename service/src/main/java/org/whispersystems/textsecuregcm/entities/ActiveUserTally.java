/**
 * Copyright (C) 2018 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
