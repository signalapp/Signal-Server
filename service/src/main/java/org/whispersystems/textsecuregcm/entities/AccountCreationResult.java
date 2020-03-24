package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

import java.util.UUID;

public class AccountCreationResult {

  @JsonProperty
  private UUID uuid;

  @JsonProperty
  private ExternalServiceCredentials backupCredentials;

  public AccountCreationResult() {}

  public AccountCreationResult(UUID uuid, ExternalServiceCredentials backupCredentials) {
    this.uuid              = uuid;
    this.backupCredentials = backupCredentials;
  }

  public UUID getUuid() {
    return uuid;
  }

  public ExternalServiceCredentials getBackupCredentials() {
    return backupCredentials;
  }
}
