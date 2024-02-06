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

public class AuthenticatedAccount implements Principal, AccountAndAuthenticatedDeviceHolder {
  private final Account account;
  private final Device device;

 public AuthenticatedAccount(final Account account, final Device device) {
    this.account = account;
    this.device = device;
  }

  @Override
  public Account getAccount() {
    return account;
  }

  @Override
  public Device getAuthenticatedDevice() {
    return device;
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
