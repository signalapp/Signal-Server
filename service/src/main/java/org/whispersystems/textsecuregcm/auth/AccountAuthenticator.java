/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.Optional;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

public class AccountAuthenticator extends BaseAccountAuthenticator implements
    Authenticator<BasicCredentials, AuthenticatedAccount> {

  public AccountAuthenticator(AccountsManager accountsManager) {
    super(accountsManager);
  }

  @Override
  public Optional<AuthenticatedAccount> authenticate(BasicCredentials basicCredentials) {
    return super.authenticate(basicCredentials, true);
  }

}
