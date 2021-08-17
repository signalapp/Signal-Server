/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.UUID;

public class DirectoryReconciliationRequest {

  @JsonProperty
  private List<User> users;

  public DirectoryReconciliationRequest() {
  }

  public DirectoryReconciliationRequest(List<User> users) {
    this.users = users;
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
      this.uuid = uuid;
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
