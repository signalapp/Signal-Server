/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.security.Principal;
import java.time.Instant;
import java.util.UUID;
import javax.security.auth.Subject;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

public class AuthenticatedDevice implements Principal, AccountAndAuthenticatedDeviceHolder {

  private final Account account;
  private final Device device;

  public AuthenticatedDevice(final Account account, final Device device) {
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

  @Override
  public UUID getAccountIdentifier() {
    return account.getIdentifier(IdentityType.ACI);
  }

  @Override
  public byte getDeviceId() {
    return device.getId();
  }

  @Override
  public Instant getPrimaryDeviceLastSeen() {
    return Instant.ofEpochMilli(account.getPrimaryDevice().getLastSeen());
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
