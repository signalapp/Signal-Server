/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.Entitlements;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;

public class AccountIdentityResponseBuilder {

  private final Account account;
  private boolean storageCapable;
  private Clock clock;

  public AccountIdentityResponseBuilder(Account account) {
    this.account = account;
    this.storageCapable = account.hasCapability(DeviceCapability.STORAGE);
    this.clock = Clock.systemUTC();
  }

  public AccountIdentityResponseBuilder storageCapable(boolean storageCapable) {
    this.storageCapable = storageCapable;
    return this;
  }

  public AccountIdentityResponseBuilder clock(Clock clock) {
    this.clock = clock;
    return this;
  }

  public AccountIdentityResponse build() {
    final List<Entitlements.BadgeEntitlement> badges = account.getBadges()
        .stream()
        .filter(bv -> bv.expiration().isAfter(clock.instant()))
        .map(badge -> new Entitlements.BadgeEntitlement(badge.id(), badge.expiration(), badge.visible()))
        .toList();

    final Entitlements.BackupEntitlement backupEntitlement = Optional
        .ofNullable(account.getBackupVoucher())
        .filter(bv -> bv.expiration().isAfter(clock.instant()))
        .map(bv -> new Entitlements.BackupEntitlement(bv.receiptLevel(), bv.expiration()))
        .orElse(null);

    return new AccountIdentityResponse(account.getUuid(),
        account.getNumber(),
        account.getPhoneNumberIdentifier(),
        account.getUsernameHash().filter(h -> h.length > 0).orElse(null),
        account.getUsernameLinkHandle(),
        storageCapable,
        new Entitlements(badges, backupEntitlement));
  }

  public static AccountIdentityResponse fromAccount(final Account account) {
    return new AccountIdentityResponseBuilder(account).build();
  }
}
