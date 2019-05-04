package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import java.util.Optional;

import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;

public class DisabledPermittedAccountAuthenticator extends BaseAccountAuthenticator implements Authenticator<BasicCredentials, DisabledPermittedAccount> {

  public DisabledPermittedAccountAuthenticator(AccountsManager accountsManager) {
    super(accountsManager);
  }
  
  @Override
  public Optional<DisabledPermittedAccount> authenticate(BasicCredentials credentials) {
    Optional<Account> account = super.authenticate(credentials, false);
    return account.map(DisabledPermittedAccount::new);
  }
}
