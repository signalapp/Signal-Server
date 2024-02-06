/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.websocket.auth.PrincipalSupplier;

public class AccountPrincipalSupplier implements PrincipalSupplier<AuthenticatedAccount> {

  private final AccountsManager accountsManager;

  public AccountPrincipalSupplier(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public AuthenticatedAccount refresh(final AuthenticatedAccount oldAccount) {
    final Account account = accountsManager.getByAccountIdentifier(oldAccount.getAccount().getUuid())
        .orElseThrow(() -> new RefreshingAccountNotFoundException("Could not find account"));
    final Device device = account.getDevice(oldAccount.getAuthenticatedDevice().getId())
        .orElseThrow(() -> new RefreshingAccountNotFoundException("Could not find device"));
    return new AuthenticatedAccount(account, device);
  }

  @Override
  public AuthenticatedAccount deepCopy(final AuthenticatedAccount authenticatedAccount) {
    final Account cloned = AccountUtil.cloneAccountAsNotStale(authenticatedAccount.getAccount());
    return new AuthenticatedAccount(
        cloned,
        cloned.getDevice(authenticatedAccount.getAuthenticatedDevice().getId())
            .orElseThrow(() -> new IllegalStateException(
                "Could not find device from a clone of an account where the device was present")));
  }
}
