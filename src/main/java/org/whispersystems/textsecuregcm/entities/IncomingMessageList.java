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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class IncomingMessageList {

  @JsonProperty
  @NotNull
  @Valid
  private List<IncomingMessage> messages;

  @JsonProperty
  private String relay;

  @JsonProperty
  private long timestamp;

  public IncomingMessageList() {}

  public List<IncomingMessage> getMessages() {
    return messages;
  }

  public String getRelay() {
    return relay;
  }

  public void setRelay(String relay) {
    this.relay = relay;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
