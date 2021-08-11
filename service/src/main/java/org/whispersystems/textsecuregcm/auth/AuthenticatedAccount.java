/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.security.Principal;
import java.util.function.Supplier;
import javax.security.auth.Subject;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

public class AuthenticatedAccount implements Principal {

  private final Supplier<Pair<Account, Device>> accountAndDevice;

  public AuthenticatedAccount(final Supplier<Pair<Account, Device>> accountAndDevice) {
    this.accountAndDevice = accountAndDevice;
  }

  public Account getAccount() {
    return accountAndDevice.get().first();
  }

  public Device getAuthenticatedDevice() {
    return accountAndDevice.get().second();
  }

  // Principal implementation

  @Override
  public String getName() {
    return null;
  }

  @Override
  public boolean implies(final Subject subject) {
    return false;
  }
}
