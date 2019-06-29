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
import java.util.UUID;

public class DirectoryReconciliationRequest {

  @JsonProperty
  private UUID fromUuid;

  @JsonProperty
  private UUID toUuid;

  @JsonProperty
  private List<UUID> uuids;

  @JsonProperty
  private List<String> numbers;

  public DirectoryReconciliationRequest() {
  }

  public DirectoryReconciliationRequest(UUID fromUuid, UUID toUuid, List<UUID> uuids, List<String> numbers) {
    this.fromUuid = fromUuid;
    this.toUuid   = toUuid;
    this.uuids    = uuids;
    this.numbers  = numbers;
  }

  public UUID getFromUuid() {
    return fromUuid;
  }

  public UUID getToUuid() {
    return toUuid;
  }

  public List<UUID> getUuids() {
    return uuids;
  }

  public List<String> getNumbers() {
    return numbers;
  }

}
