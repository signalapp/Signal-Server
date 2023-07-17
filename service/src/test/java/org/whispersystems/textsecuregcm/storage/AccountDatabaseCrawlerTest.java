/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountDatabaseCrawlerTest {

  private static final UUID ACCOUNT1 = UUID.randomUUID();
  private static final UUID ACCOUNT2 = UUID.randomUUID();

  private static final int CHUNK_SIZE = 1000;

  private final Account account1 = mock(Account.class);
  private final Account account2 = mock(Account.class);

  private final AccountsManager accounts = mock(AccountsManager.class);
  private final AccountDatabaseCrawlerListener listener = mock(AccountDatabaseCrawlerListener.class);
  private final AccountDatabaseCrawlerCache cache = mock(AccountDatabaseCrawlerCache.class);

  private final AccountDatabaseCrawler crawler =
      new AccountDatabaseCrawler("test", accounts, cache, List.of(listener), CHUNK_SIZE);

  @BeforeEach
  void setup() {
    when(account1.getUuid()).thenReturn(ACCOUNT1);
    when(account2.getUuid()).thenReturn(ACCOUNT2);

    when(accounts.getAllFromDynamo(anyInt())).thenReturn(
        new AccountCrawlChunk(List.of(account1, account2), ACCOUNT2));
    when(accounts.getAllFromDynamo(eq(ACCOUNT1), anyInt())).thenReturn(
        new AccountCrawlChunk(List.of(account2), ACCOUNT2));
    when(accounts.getAllFromDynamo(eq(ACCOUNT2), anyInt())).thenReturn(
        new AccountCrawlChunk(Collections.emptyList(), null));

    when(cache.claimActiveWork(any(), anyLong())).thenReturn(true);
  }

  @Test
  void testCrawlAllAccounts() {
    when(cache.getLastUuid())
        .thenReturn(Optional.empty());

    crawler.crawlAllAccounts();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(listener, times(1)).onCrawlStart();
    verify(accounts, times(1)).getAllFromDynamo(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT2), eq(CHUNK_SIZE));
    verify(listener, times(1)).timeAndProcessCrawlChunk(Optional.empty(), List.of(account1, account2));
    verify(listener, times(1)).timeAndProcessCrawlChunk(Optional.of(ACCOUNT2), Collections.emptyList());
    verify(listener, times(1)).onCrawlEnd();
    verify(cache, times(1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    // times(2) because empty() will get cached on the last run of loop and then again at the end
    verify(cache, times(1)).setLastUuid(eq(Optional.empty()));
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

}
