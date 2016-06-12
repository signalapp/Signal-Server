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
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

public class TwilioConfiguration {

  @NotEmpty
  @JsonProperty
  private String accountId;

  @NotEmpty
  @JsonProperty
  private String accountToken;

  @NotNull
  @JsonProperty
  private List<String> numbers;

  @NotEmpty
  @JsonProperty
  private String localDomain;

  @JsonProperty
  private String messagingServicesId;

  public String getAccountId() {
    return accountId;
  }

  public String getAccountToken() {
    return accountToken;
  }

  public List<String> getNumbers() {
    return numbers;
  }

  public String getLocalDomain() {
    return localDomain;
  }

  public String getMessagingServicesId() {
    return messagingServicesId;
  }
}
