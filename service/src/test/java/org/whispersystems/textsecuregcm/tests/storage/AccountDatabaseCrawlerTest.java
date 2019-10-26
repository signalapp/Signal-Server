/*
 * Copyright (C) 2018 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class AccountDatabaseCrawlerTest {

  private static final UUID ACCOUNT1 = UUID.randomUUID();
  private static final UUID ACCOUNT2 = UUID.randomUUID();

  private static final int  CHUNK_SIZE        = 1000;
  private static final long CHUNK_INTERVAL_MS = 30_000L;

  private final Account account1 = mock(Account.class);
  private final Account account2 = mock(Account.class);

  private final AccountsManager                accounts = mock(AccountsManager.class);
  private final AccountDatabaseCrawlerListener listener = mock(AccountDatabaseCrawlerListener.class);
  private final AccountDatabaseCrawlerCache    cache    = mock(AccountDatabaseCrawlerCache.class);

  private final AccountDatabaseCrawler        crawler   = new AccountDatabaseCrawler(accounts, cache, Arrays.asList(listener), CHUNK_SIZE, CHUNK_INTERVAL_MS);

  @Before
  public void setup() {
    when(account1.getUuid()).thenReturn(ACCOUNT1);
    when(account2.getUuid()).thenReturn(ACCOUNT2);

    when(accounts.getAllFrom(anyInt())).thenReturn(Arrays.asList(account1, account2));
    when(accounts.getAllFrom(eq(ACCOUNT1), anyInt())).thenReturn(Arrays.asList(account2));
    when(accounts.getAllFrom(eq(ACCOUNT2), anyInt())).thenReturn(Collections.emptyList());

    when(cache.claimActiveWork(any(), anyLong())).thenReturn(true);
    when(cache.isAccelerated()).thenReturn(false);
  }

  @Test
  public void testCrawlStart() throws AccountDatabaseCrawlerRestartException {
    when(cache.getLastUuid()).thenReturn(Optional.empty());

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(listener, times(1)).onCrawlStart();
    verify(accounts, times(1)).getAllFrom(eq(CHUNK_SIZE));
    verify(accounts, times(0)).getAllFrom(any(UUID.class), eq(CHUNK_SIZE));
    verify(account1, times(0)).getUuid();
    verify(account2, times(1)).getUuid();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.empty()), eq(Arrays.asList(account1, account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoMoreInteractions(account1);
    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void testCrawlChunk() throws AccountDatabaseCrawlerRestartException {
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFrom(eq(ACCOUNT1), eq(CHUNK_SIZE));
    verify(account2, times(1)).getUuid();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void testCrawlChunkAccelerated() throws AccountDatabaseCrawlerRestartException {
    when(cache.isAccelerated()).thenReturn(true);
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isTrue();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFrom(eq(ACCOUNT1), eq(CHUNK_SIZE));
    verify(account2, times(1)).getUuid();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void testCrawlChunkRestart() throws AccountDatabaseCrawlerRestartException {
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));
    doThrow(AccountDatabaseCrawlerRestartException.class).when(listener).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFrom(eq(ACCOUNT1), eq(CHUNK_SIZE));
    verify(account2, times(0)).getNumber();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.empty()));
    verify(cache, times(1)).clearAccelerate();
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @Test
  public void testCrawlEnd() {
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT2));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(1)).getLastUuid();
    verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
    verify(accounts, times(1)).getAllFrom(eq(ACCOUNT2), eq(CHUNK_SIZE));
    verify(account1, times(0)).getNumber();
    verify(account2, times(0)).getNumber();
    verify(listener, times(1)).onCrawlEnd(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).setLastUuid(eq(Optional.empty()));
    verify(cache, times(1)).clearAccelerate();
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);
    verifyZeroInteractions(account2);

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

}
