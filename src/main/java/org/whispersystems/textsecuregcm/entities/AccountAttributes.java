/**
 * Copyright (C) 2013 Open WhisperSystems
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
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;

public class AccountAttributes {

  @JsonProperty
  @NotEmpty
  private String signalingKey;

  @JsonProperty
  private boolean fetchesMessages;

  @JsonProperty
  private int registrationId;

  @JsonProperty
  @Length(max = 50, message = "This field must be less than 50 characters")
  private String name;

  public AccountAttributes() {}

  @VisibleForTesting
  public AccountAttributes(String signalingKey, boolean fetchesMessages, int registrationId) {
    this(signalingKey, fetchesMessages, registrationId, null);
  }

  @VisibleForTesting
  public AccountAttributes(String signalingKey, boolean fetchesMessages, int registrationId, String name) {
    this.signalingKey    = signalingKey;
    this.fetchesMessages = fetchesMessages;
    this.registrationId  = registrationId;
    this.name            = name;
  }

  public String getSignalingKey() {
    return signalingKey;
  }

  public boolean getFetchesMessages() {
    return fetchesMessages;
  }

  public int getRegistrationId() {
    return registrationId;
  }

  public String getName() {
    return name;
  }
}
