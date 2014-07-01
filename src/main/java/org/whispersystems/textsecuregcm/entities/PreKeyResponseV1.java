/**
 * Copyright (C) 2014 Open WhisperSystems
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PreKeyResponseV1 {

  @JsonProperty
  @NotNull
  @Valid
  private List<PreKeyV1> keys;

  @VisibleForTesting
  public PreKeyResponseV1() {}

  public PreKeyResponseV1(PreKeyV1 preKey) {
    this.keys = new LinkedList<>();
    this.keys.add(preKey);
  }

  public PreKeyResponseV1(List<PreKeyV1> preKeys) {
    this.keys = preKeys;
  }

  public List<PreKeyV1> getKeys() {
    return keys;
  }

  @VisibleForTesting
  public boolean equals(Object o) {
    if (!(o instanceof PreKeyResponseV1) ||
        ((PreKeyResponseV1) o).keys.size() != keys.size())
      return false;
    Iterator<PreKeyV1> otherKeys = ((PreKeyResponseV1) o).keys.iterator();
    for (PreKeyV1 key : keys) {
      if (!otherKeys.next().equals(key))
        return false;
    }
    return true;
  }

  public int hashCode() {
    int ret = 0xFBA4C795 * keys.size();
    for (PreKeyV1 key : keys)
      ret ^= key.getPublicKey().hashCode();
    return ret;
  }
}
