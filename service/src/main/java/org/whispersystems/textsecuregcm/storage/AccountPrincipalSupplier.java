/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.websocket.auth.PrincipalSupplier;

public class AccountPrincipalSupplier implements PrincipalSupplier<AuthenticatedDevice> {

  private final AccountsManager accountsManager;

  public AccountPrincipalSupplier(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public AuthenticatedDevice refresh(final AuthenticatedDevice oldAccount) {
    final Account account = accountsManager.getByAccountIdentifier(oldAccount.getAccount().getUuid())
        .orElseThrow(() -> new RefreshingAccountNotFoundException("Could not find account"));
    final Device device = account.getDevice(oldAccount.getAuthenticatedDevice().getId())
        .orElseThrow(() -> new RefreshingAccountNotFoundException("Could not find device"));
    return new AuthenticatedDevice(account, device);
  }

  @Override
  public AuthenticatedDevice deepCopy(final AuthenticatedDevice authenticatedDevice) {
    final Account cloned = AccountUtil.cloneAccountAsNotStale(authenticatedDevice.getAccount());
    return new AuthenticatedDevice(
        cloned,
        cloned.getDevice(authenticatedDevice.getAuthenticatedDevice().getId())
            .orElseThrow(() -> new IllegalStateException(
                "Could not find device from a clone of an account where the device was present")));
  }
}
