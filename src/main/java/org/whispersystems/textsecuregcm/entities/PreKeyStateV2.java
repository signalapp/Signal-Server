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

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class PreKeyStateV2 {

  @JsonProperty
  @NotNull
  @Valid
  private List<PreKeyV2> preKeys;

  @JsonProperty
  @NotNull
  @Valid
  private SignedPreKey signedPreKey;

  @JsonProperty
  @NotNull
  @Valid
  private PreKeyV2 lastResortKey;

  @JsonProperty
  @NotEmpty
  private String identityKey;

  public PreKeyStateV2() {}

  @VisibleForTesting
  public PreKeyStateV2(String identityKey, SignedPreKey signedPreKey,
                       List<PreKeyV2> keys, PreKeyV2 lastResortKey)
  {
    this.identityKey   = identityKey;
    this.signedPreKey  = signedPreKey;
    this.preKeys       = keys;
    this.lastResortKey = lastResortKey;
  }

  public List<PreKeyV2> getPreKeys() {
    return preKeys;
  }

  public SignedPreKey getSignedPreKey() {
    return signedPreKey;
  }

  public String getIdentityKey() {
    return identityKey;
  }

  public PreKeyV2 getLastResortKey() {
    return lastResortKey;
  }
}
