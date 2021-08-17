/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.security.Principal;
import javax.security.auth.Subject;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

public class DisabledPermittedAuthenticatedAccount implements Principal, AccountAndAuthenticatedDeviceHolder {

  private final AuthenticatedAccount authenticatedAccount;

  public DisabledPermittedAuthenticatedAccount(final AuthenticatedAccount authenticatedAccount) {
    this.authenticatedAccount = authenticatedAccount;
  }

  @Override
  public Account getAccount() {
    return authenticatedAccount.getAccount();
  }

  @Override
  public Device getAuthenticatedDevice() {
    return authenticatedAccount.getAuthenticatedDevice();
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
