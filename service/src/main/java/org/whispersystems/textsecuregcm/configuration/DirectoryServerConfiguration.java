/**
 * Copyright (C) 2018 Open WhisperSystems
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
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class DirectoryServerConfiguration {

  @NotEmpty
  @JsonProperty
  private String replicationUrl;

  @NotEmpty
  @JsonProperty
  private String replicationPassword;

  @NotEmpty
  @JsonProperty
  private String replicationCaCertificate;

  public String getReplicationUrl() {
    return replicationUrl;
  }

  public String getReplicationPassword() {
    return replicationPassword;
  }

  public String getReplicationCaCertificate() {
    return replicationCaCertificate;
  }

}
