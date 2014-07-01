/**
 * Copyright (C) 2014 Open Whisper Systems
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
import com.google.common.annotations.VisibleForTesting;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class PreKeyStateV1 {

  @JsonProperty
  @NotNull
  @Valid
  private PreKeyV1 lastResortKey;

  @JsonProperty
  @NotNull
  @Valid
  private List<PreKeyV1> keys;

  public List<PreKeyV1> getKeys() {
    return keys;
  }

  @VisibleForTesting
  public void setKeys(List<PreKeyV1> keys) {
    this.keys = keys;
  }

  public PreKeyV1 getLastResortKey() {
    return lastResortKey;
  }

  @VisibleForTesting
  public void setLastResortKey(PreKeyV1 lastResortKey) {
    this.lastResortKey = lastResortKey;
  }
}
