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
import org.hibernate.validator.constraints.NotEmpty;

public class IncomingMessage {

  @JsonProperty
  private int    type;

  @JsonProperty
  private String destination;

  @JsonProperty
  private long   destinationDeviceId = 1;

  @JsonProperty
  private int destinationRegistrationId;

  @JsonProperty
  private String body;

  @JsonProperty
  private String content;

  @JsonProperty
  private String relay;

  @JsonProperty
  private long   timestamp; // deprecated

  @JsonProperty
  private boolean silent = false;


  public String getDestination() {
    return destination;
  }

  public String getBody() {
    return body;
  }

  public int getType() {
    return type;
  }

  public String getRelay() {
    return relay;
  }

  public long getDestinationDeviceId() {
    return destinationDeviceId;
  }

  public int getDestinationRegistrationId() {
    return destinationRegistrationId;
  }

  public String getContent() {
    return content;
  }

  public boolean isSilent() {
    return silent;
  }
}
