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

public class PreKeyState {

  @JsonProperty
  @NotNull
  @Valid
  private List<PreKey> preKeys;

  @JsonProperty
  @NotNull
  @Valid
  private SignedPreKey signedPreKey;

  @JsonProperty
  @NotEmpty
  private String identityKey;

  public PreKeyState() {}

  @VisibleForTesting
  public PreKeyState(String identityKey, SignedPreKey signedPreKey, List<PreKey> keys) {
    this.identityKey   = identityKey;
    this.signedPreKey  = signedPreKey;
    this.preKeys       = keys;
  }

  public List<PreKey> getPreKeys() {
    return preKeys;
  }

  public SignedPreKey getSignedPreKey() {
    return signedPreKey;
  }

  public String getIdentityKey() {
    return identityKey;
  }

}
