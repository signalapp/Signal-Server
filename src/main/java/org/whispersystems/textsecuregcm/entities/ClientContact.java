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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import java.util.Arrays;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ClientContact {

  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  @JsonProperty
  private byte[]  token;

  private String  relay;
  private boolean inactive;

  public ClientContact(byte[] token, String relay) {
    this.token = token;
    this.relay = relay;
  }

  public ClientContact() {}

  public byte[] getToken() {
    return token;
  }

  public String getRelay() {
    return relay;
  }

  public void setRelay(String relay) {
    this.relay = relay;
  }

  public boolean isInactive() {
    return inactive;
  }

  public void setInactive(boolean inactive) {
    this.inactive = inactive;
  }

//  public String toString() {
//    return new Gson().toJson(this);
//  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if (!(other instanceof ClientContact)) return false;

    ClientContact that = (ClientContact)other;

    return
        Arrays.equals(this.token, that.token) &&
        this.inactive == that.inactive &&
        (this.relay == null ? (that.relay == null) : this.relay.equals(that.relay));
  }

  public int hashCode() {
    return Arrays.hashCode(this.token);
  }

}
