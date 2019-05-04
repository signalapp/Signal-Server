package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.storage.Account;

import javax.security.auth.Subject;
import java.security.Principal;

public class DisabledPermittedAccount implements Principal  {

  private final Account account;

  public DisabledPermittedAccount(Account account) {
    this.account = account;
  }

  public Account getAccount() {
    return account;
  }

  // Principal implementation

  @Override
  public String getName() {
    return null;
  }

  @Override
  public boolean implies(Subject subject) {
    return false;
  }
}
