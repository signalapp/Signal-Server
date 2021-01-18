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
    return super.authenticate(credentials, false)
            .map(DisabledPermittedAccount::new);
  }
}
