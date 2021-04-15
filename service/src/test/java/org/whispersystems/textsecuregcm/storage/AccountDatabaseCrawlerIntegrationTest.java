/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AccountDatabaseCrawlerIntegrationTest extends AbstractRedisClusterTest {

  private static final UUID FIRST_UUID = UUID.fromString("82339e80-81cd-48e2-9ed2-ccd5dd262ad9");
  private static final UUID SECOND_UUID = UUID.fromString("cc705c84-33cf-456b-8239-a6a34e2f561a");

  private Account firstAccount;
  private Account secondAccount;

  private AccountsManager accountsManager;
  private AccountDatabaseCrawlerListener listener;

  private AccountDatabaseCrawler accountDatabaseCrawler;

  private static final int CHUNK_SIZE = 1;
  private static final long CHUNK_INTERVAL_MS = 0;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    firstAccount = mock(Account.class);
    secondAccount = mock(Account.class);

    accountsManager = mock(AccountsManager.class);
    listener = mock(AccountDatabaseCrawlerListener.class);

    when(firstAccount.getUuid()).thenReturn(FIRST_UUID);
    when(secondAccount.getUuid()).thenReturn(SECOND_UUID);

    when(accountsManager.getAllFrom(CHUNK_SIZE)).thenReturn(List.of(firstAccount));
    when(accountsManager.getAllFrom(FIRST_UUID, CHUNK_SIZE))
        .thenReturn(List.of(secondAccount))
        .thenReturn(Collections.emptyList());

    final AccountDatabaseCrawlerCache crawlerCache = new AccountDatabaseCrawlerCache(getRedisCluster());
    accountDatabaseCrawler = new AccountDatabaseCrawler(accountsManager, crawlerCache, List.of(listener), CHUNK_SIZE, CHUNK_INTERVAL_MS);
  }

  @Test
  public void testCrawlUninterrupted() throws AccountDatabaseCrawlerRestartException {
    assertFalse(accountDatabaseCrawler.doPeriodicWork());
    assertFalse(accountDatabaseCrawler.doPeriodicWork());
    assertFalse(accountDatabaseCrawler.doPeriodicWork());

    verify(accountsManager).getAllFrom(CHUNK_SIZE);
    verify(accountsManager).getAllFrom(FIRST_UUID, CHUNK_SIZE);
    verify(accountsManager).getAllFrom(SECOND_UUID, CHUNK_SIZE);

    verify(listener).onCrawlStart();
    verify(listener).timeAndProcessCrawlChunk(Optional.empty(), List.of(firstAccount));
    verify(listener).timeAndProcessCrawlChunk(Optional.of(FIRST_UUID), List.of(secondAccount));
    verify(listener).onCrawlEnd(Optional.of(SECOND_UUID));
  }

  @Test
  public void testCrawlWithReset() throws AccountDatabaseCrawlerRestartException {
    doThrow(new AccountDatabaseCrawlerRestartException("OH NO")).doNothing()
        .when(listener).timeAndProcessCrawlChunk(Optional.empty(), List.of(firstAccount));

    assertFalse(accountDatabaseCrawler.doPeriodicWork());
    assertFalse(accountDatabaseCrawler.doPeriodicWork());
    assertFalse(accountDatabaseCrawler.doPeriodicWork());
    assertFalse(accountDatabaseCrawler.doPeriodicWork());

    verify(accountsManager, times(2)).getAllFrom(CHUNK_SIZE);
    verify(accountsManager).getAllFrom(FIRST_UUID, CHUNK_SIZE);
    verify(accountsManager).getAllFrom(SECOND_UUID, CHUNK_SIZE);

    verify(listener, times(2)).onCrawlStart();
    verify(listener, times(2)).timeAndProcessCrawlChunk(Optional.empty(), List.of(firstAccount));
    verify(listener).timeAndProcessCrawlChunk(Optional.of(FIRST_UUID), List.of(secondAccount));
    verify(listener).onCrawlEnd(Optional.of(SECOND_UUID));
  }
}
