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
package org.whispersystems.textsecuregcm.federation;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.URL;

public class FederatedPeer {

  @NotEmpty
  @JsonProperty
  private String name;

  @NotEmpty
  @URL
  @JsonProperty
  private String url;

  @NotEmpty
  @JsonProperty
  private String authenticationToken;

  @NotEmpty
  @JsonProperty
  private String certificate;

  public FederatedPeer() {}

  public FederatedPeer(String name, String url, String authenticationToken, String certificate) {
    this.name                = name;
    this.url                 = url;
    this.authenticationToken = authenticationToken;
    this.certificate         = certificate;
  }

  public String getUrl() {
    return url;
  }

  public String getName() {
    return name;
  }

  public String getAuthenticationToken() {
    return authenticationToken;
  }

  public String getCertificate() {
    return certificate;
  }
}
