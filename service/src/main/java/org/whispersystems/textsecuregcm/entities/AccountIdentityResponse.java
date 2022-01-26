/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.UUID;

public class AccountIdentityResponse {

  private final UUID uuid;
  private final String number;
  private final UUID pni;

  @Nullable
  private final String username;

  private final boolean storageCapable;

  @JsonCreator
  public AccountIdentityResponse(
      @JsonProperty("uuid") final UUID uuid,
      @JsonProperty("number") final String number,
      @JsonProperty("pni") final UUID pni,
      @JsonProperty("username") @Nullable final String username,
      @JsonProperty("storageCapable") final boolean storageCapable) {

    this.uuid = uuid;
    this.number = number;
    this.pni = pni;
    this.username = username;
    this.storageCapable = storageCapable;
  }

  public UUID getUuid() {
    return uuid;
  }

  public String getNumber() {
    return number;
  }

  public UUID getPni() {
    return pni;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  public boolean isStorageCapable() {
    return storageCapable;
  }
}
