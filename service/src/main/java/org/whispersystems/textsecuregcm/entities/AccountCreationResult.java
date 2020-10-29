/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

import java.util.UUID;

public class AccountCreationResult {

  @JsonProperty
  private UUID uuid;

  @JsonProperty
  private boolean storageCapable;

  public AccountCreationResult() {}

  public AccountCreationResult(UUID uuid, boolean storageCapable) {
    this.uuid           = uuid;
    this.storageCapable = storageCapable;
  }

  public UUID getUuid() {
    return uuid;
  }

  public boolean isStorageCapable() {
    return storageCapable;
  }
}
