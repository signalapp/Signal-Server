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

  @JsonProperty
  private boolean voice;

  @JsonProperty
  private boolean video;

  @JsonProperty
  private String pin;

  public AccountAttributes() {}

  @VisibleForTesting
  public AccountAttributes(String signalingKey, boolean fetchesMessages, int registrationId, String pin) {
    this(signalingKey, fetchesMessages, registrationId, null, false, false, pin);
  }

  @VisibleForTesting
  public AccountAttributes(String signalingKey, boolean fetchesMessages, int registrationId, String name, boolean voice, boolean video, String pin) {
    this.signalingKey    = signalingKey;
    this.fetchesMessages = fetchesMessages;
    this.registrationId  = registrationId;
    this.name            = name;
    this.voice           = voice;
    this.video           = video;
    this.pin             = pin;
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

  public boolean getVoice() {
    return voice;
  }

  public boolean getVideo() {
    return video;
  }

  public String getPin() {
    return pin;
  }
}
