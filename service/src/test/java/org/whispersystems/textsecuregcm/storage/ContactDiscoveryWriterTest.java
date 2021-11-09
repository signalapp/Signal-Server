/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ContactDiscoveryWriterTest {

  @ParameterizedTest
  @MethodSource
  void testUpdatesOnChange(boolean canonicallyDiscoverable, boolean shouldBeVisible, boolean updateCalled) throws AccountDatabaseCrawlerRestartException {
    AccountsManager mgr = mock(AccountsManager.class);
    UUID uuid = UUID.randomUUID();
    Account acct = mock(Account.class);
    when(acct.getUuid()).thenReturn(uuid);
    when(acct.isCanonicallyDiscoverable()).thenReturn(canonicallyDiscoverable);
    when(acct.shouldBeVisibleInDirectory()).thenReturn(shouldBeVisible);
    when(mgr.getByAccountIdentifier(uuid)).thenReturn(Optional.of(acct));
    ContactDiscoveryWriter writer = new ContactDiscoveryWriter(mgr);
    writer.onCrawlChunk(Optional.empty(), List.of(acct));
    verify(mgr, times(updateCalled ? 1 : 0)).update(acct, ContactDiscoveryWriter.NOOP_UPDATER);
  }

  static Stream<Arguments> testUpdatesOnChange() {
    return Stream.of(
        Arguments.of(true, true, false),
        Arguments.of(false, false, false),
        Arguments.of(true, false, true),
        Arguments.of(false, true, true));
  }
}
