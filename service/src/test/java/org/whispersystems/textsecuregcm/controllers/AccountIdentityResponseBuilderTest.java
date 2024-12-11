/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.Entitlements;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.util.TestClock;

class AccountIdentityResponseBuilderTest {

  @Test
  void expiredBackupEntitlement() {
    final Instant expiration = Instant.ofEpochSecond(101);
    final Account account = mock(Account.class);
    when(account.getBackupVoucher()).thenReturn(new Account.BackupVoucher(6, expiration));

    Entitlements.BackupEntitlement backup = new AccountIdentityResponseBuilder(account)
        .clock(TestClock.pinned(Instant.ofEpochSecond(101)))
        .build().entitlements().backup();
    assertThat(backup).isNull();

    backup = new AccountIdentityResponseBuilder(account)
        .clock(TestClock.pinned(Instant.ofEpochSecond(100)))
        .build().entitlements().backup();
    assertThat(backup).isNotNull();
    assertThat(backup.expiration()).isEqualTo(expiration);
    assertThat(backup.backupLevel()).isEqualTo(6);
  }

  @Test
  void expiredBadgeEntitlement() {
    final Account account = mock(Account.class);
    when(account.getBadges()).thenReturn(List.of(
        new AccountBadge("badge1", Instant.ofEpochSecond(10), false),
        new AccountBadge("badge2", Instant.ofEpochSecond(11), true)));

    // all should be expired
    assertThat(new AccountIdentityResponseBuilder(account)
        .clock(TestClock.pinned(Instant.ofEpochSecond(11)))
        .build().entitlements().badges()).isEmpty();

    // first badge should be expired
    assertThat(new AccountIdentityResponseBuilder(account).clock(TestClock.pinned(Instant.ofEpochSecond(10))).build()
        .entitlements()
        .badges()
        .stream().map(Entitlements.BadgeEntitlement::id).toList())
        .containsExactly("badge2");

    // no badges should be expired
    assertThat(new AccountIdentityResponseBuilder(account).clock(TestClock.pinned(Instant.ofEpochSecond(9))).build()
        .entitlements()
        .badges()
        .stream().map(Entitlements.BadgeEntitlement::id).toList())
        .containsExactly("badge1", "badge2");
  }

}
