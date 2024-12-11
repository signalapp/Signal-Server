/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;

public class AccountIdentityResponseBuilder {

  private final Account account;
  private boolean storageCapable;

  public AccountIdentityResponseBuilder(Account account) {
    this.account = account;
    this.storageCapable = account.hasCapability(DeviceCapability.STORAGE);
  }

  public AccountIdentityResponseBuilder storageCapable(boolean storageCapable) {
    this.storageCapable = storageCapable;
    return this;
  }

  public AccountIdentityResponse build() {
    return new AccountIdentityResponse(account.getUuid(),
        account.getNumber(),
        account.getPhoneNumberIdentifier(),
        account.getUsernameHash().filter(h -> h.length > 0).orElse(null),
        account.getUsernameLinkHandle(),
        storageCapable);
  }

  public static AccountIdentityResponse fromAccount(final Account account) {
    return new AccountIdentityResponseBuilder(account).build();
  }
}
