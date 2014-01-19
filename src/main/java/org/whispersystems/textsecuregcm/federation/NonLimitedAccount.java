package org.whispersystems.textsecuregcm.federation;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.storage.Account;

public class NonLimitedAccount extends Account {

  @JsonIgnore
  private final String number;

  @JsonIgnore
  private final String relay;

  public NonLimitedAccount(String number, String relay) {
    this.number = number;
    this.relay  = relay;
  }

  public String getNumber() {
    return number;
  }

  public boolean isRateLimited() {
    return false;
  }

  public Optional<String> getRelay() {
    return Optional.of(relay);
  }
}
