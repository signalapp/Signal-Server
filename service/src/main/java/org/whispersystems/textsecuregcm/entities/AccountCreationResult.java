/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class AccountCreationResult {

  private final UUID uuid;
  private final String number;
  private final UUID pni;
  private final boolean storageCapable;

  @JsonCreator
  public AccountCreationResult(
      @JsonProperty("uuid") final UUID uuid,
      @JsonProperty("number") final String number,
      @JsonProperty("pni") final UUID pni,
      @JsonProperty("storageCapable") final boolean storageCapable) {

    this.uuid = uuid;
    this.number = number;
    this.pni = pni;
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

  public boolean isStorageCapable() {
    return storageCapable;
  }
}
