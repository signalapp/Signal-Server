/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

import java.util.UUID;

public class AccountCreationResult {

  @JsonProperty
  private final UUID uuid;

  @JsonProperty
  private final String number;

  @JsonProperty
  private final boolean storageCapable;

  @JsonCreator
  public AccountCreationResult(
      @JsonProperty("uuid") final UUID uuid,
      @JsonProperty("number") final String number,
      @JsonProperty("storageCapable") final boolean storageCapable) {

    this.uuid = uuid;
    this.number = number;
    this.storageCapable = storageCapable;
  }

  public UUID getUuid() {
    return uuid;
  }

  public String getNumber() {
    return number;
  }

  public boolean isStorageCapable() {
    return storageCapable;
  }
}
