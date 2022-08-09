/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import static org.mockito.Mockito.*;

class UsernameCleanerTest {

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final Account hasUsername = mock(Account.class);
  private final Account noUsername = mock(Account.class);


  @BeforeEach
  void setup() {
    when(hasUsername.getUsername()).thenReturn(Optional.of("n00bkiller"));
    when(noUsername.getUsername()).thenReturn(Optional.empty());
  }

  @Test
  void testAccounts() throws AccountDatabaseCrawlerRestartException {
    UsernameCleaner accountCleaner = new UsernameCleaner(accountsManager);
    accountCleaner.onCrawlStart();
    accountCleaner.timeAndProcessCrawlChunk(Optional.empty(), Arrays.asList(hasUsername, noUsername));
    accountCleaner.onCrawlEnd(Optional.empty());
    verify(accountsManager).clearUsername(hasUsername);
    verify(accountsManager, never()).clearUsername(noUsername);
  }
}
