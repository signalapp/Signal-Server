package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;

public class RegistrationLockFailure {

  @JsonProperty
  private long timeRemaining;

  @JsonProperty
  private ExternalServiceCredentials storageCredentials;

  public RegistrationLockFailure() {}

  public RegistrationLockFailure(long timeRemaining, ExternalServiceCredentials storageCredentials) {
    this.timeRemaining      = timeRemaining;
    this.storageCredentials = storageCredentials;
  }

  @JsonIgnore
  public long getTimeRemaining() {
    return timeRemaining;
  }

  @JsonIgnore
  public ExternalServiceCredentials getStorageCredentials() {
    return storageCredentials;
  }
}
