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

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

public class DirectoryReconciliationRequest {

  @JsonProperty
  private UUID fromUuid;

  @JsonProperty
  private UUID toUuid;

  @JsonProperty
  private List<User> users;

  public DirectoryReconciliationRequest() {
  }

  public DirectoryReconciliationRequest(UUID fromUuid, UUID toUuid, List<User> users) {
    this.fromUuid = fromUuid;
    this.toUuid   = toUuid;
    this.users    = users;
  }

  public UUID getFromUuid() {
    return fromUuid;
  }

  public UUID getToUuid() {
    return toUuid;
  }

  public List<User> getUsers() {
    return users;
  }

  public static class User {

    @JsonProperty
    private UUID uuid;

    @JsonProperty
    private String number;

    public User() {
    }

    public User(UUID uuid, String number) {
      this.uuid   = uuid;
      this.number = number;
    }

    public UUID getUuid() {
      return uuid;
    }

    public String getNumber() {
      return number;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      User user = (User) o;

      if (uuid != null ? !uuid.equals(user.uuid) : user.uuid != null) return false;
      if (number != null ? !number.equals(user.number) : user.number != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = uuid != null ? uuid.hashCode() : 0;
      result = 31 * result + (number != null ? number.hashCode() : 0);
      return result;
    }
  }
}
