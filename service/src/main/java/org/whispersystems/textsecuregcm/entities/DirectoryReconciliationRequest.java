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
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DirectoryReconciliationRequest {

  @JsonProperty
  private String fromNumber;

  @JsonProperty
  private String toNumber;

  @JsonProperty
  private List<String> numbers;

  public DirectoryReconciliationRequest() {
  }

  public DirectoryReconciliationRequest(String fromNumber, String toNumber, List<String> numbers) {
    this.fromNumber = fromNumber;
    this.toNumber   = toNumber;
    this.numbers    = numbers;
  }

  public String getFromNumber() {
    return fromNumber;
  }

  public String getToNumber() {
    return toNumber;
  }

  public List<String> getNumbers() {
    return numbers;
  }

}
