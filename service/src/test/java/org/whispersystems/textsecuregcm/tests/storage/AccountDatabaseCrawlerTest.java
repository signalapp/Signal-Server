/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountCrawlChunk;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

class AccountDatabaseCrawlerTest {

  private static final UUID ACCOUNT1 = UUID.randomUUID();
  private static final UUID ACCOUNT2 = UUID.randomUUID();

  private static final int CHUNK_SIZE = 1000;
  private static final long CHUNK_INTERVAL_MS = 30_000L;

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
  void testCrawlStart() throws AccountDatabaseCrawlerRestartException {
    when(cache.getLastUuid()).thenReturn(Optional.empty());

    crawler.doPeriodicWork();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(listener, times(1)).onCrawlStart();
    verify(accounts, times(1)).getAllFromDynamo(eq(CHUNK_SIZE));
    verify(accounts, times(0)).getAllFromDynamo(any(UUID.class), eq(CHUNK_SIZE));
    verify(account1, times(0)).getUuid();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.empty()), eq(List.of(account1, account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoMoreInteractions(account1);
    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  void testCrawlChunk() throws AccountDatabaseCrawlerRestartException {
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));

    crawler.doPeriodicWork();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT1), eq(CHUNK_SIZE));
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(List.of(account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  void testCrawlChunkAccelerated() throws AccountDatabaseCrawlerRestartException {
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));

    crawler.doPeriodicWork();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT1), eq(CHUNK_SIZE));
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(List.of(account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  void testCrawlChunkRestart() throws AccountDatabaseCrawlerRestartException {
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));
    doThrow(AccountDatabaseCrawlerRestartException.class).when(listener)
        .timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(List.of(account2)));

    crawler.doPeriodicWork();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT1), eq(CHUNK_SIZE));
    verify(account2, times(0)).getNumber();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(List.of(account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.empty()));
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  void testCrawlEnd() {
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT2));

    crawler.doPeriodicWork();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT2), eq(CHUNK_SIZE));
    verify(account1, times(0)).getNumber();
    verify(account2, times(0)).getNumber();
    verify(listener, times(1)).onCrawlEnd(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.empty()));
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoInteractions(account1);
    verifyNoInteractions(account2);

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

}
