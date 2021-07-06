/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicAccountsDynamoDbMigrationConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountCrawlChunk;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class AccountDatabaseCrawlerTest {

  private static final UUID ACCOUNT1 = UUID.randomUUID();
  private static final UUID ACCOUNT2 = UUID.randomUUID();

  private static final int  CHUNK_SIZE        = 1000;
  private static final long CHUNK_INTERVAL_MS = 30_000L;

  private final Account account1 = mock(Account.class);
  private final Account account2 = mock(Account.class);

  private final AccountsManager                accounts = mock(AccountsManager.class);
  private final AccountDatabaseCrawlerListener listener = mock(AccountDatabaseCrawlerListener.class);
  private final AccountDatabaseCrawlerCache    cache    = mock(AccountDatabaseCrawlerCache.class);

  private final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

  private final AccountDatabaseCrawler        crawler   = new AccountDatabaseCrawler(accounts, cache, Arrays.asList(listener), CHUNK_SIZE, CHUNK_INTERVAL_MS, dynamicConfigurationManager);
  private DynamicAccountsDynamoDbMigrationConfiguration dynamicAccountsDynamoDbMigrationConfiguration;

  @BeforeEach
  void setup() {
    when(account1.getUuid()).thenReturn(ACCOUNT1);
    when(account2.getUuid()).thenReturn(ACCOUNT2);

    when(accounts.getAllFrom(anyInt())).thenReturn(new AccountCrawlChunk(Arrays.asList(account1, account2), ACCOUNT2));
    when(accounts.getAllFrom(eq(ACCOUNT1), anyInt())).thenReturn(new AccountCrawlChunk(Arrays.asList(account2), ACCOUNT2));
    when(accounts.getAllFrom(eq(ACCOUNT2), anyInt())).thenReturn(new AccountCrawlChunk(Collections.emptyList(), null));

    when(accounts.getAllFromDynamo(anyInt())).thenReturn(new AccountCrawlChunk(Arrays.asList(account1, account2), ACCOUNT2));
    when(accounts.getAllFromDynamo(eq(ACCOUNT1), anyInt())).thenReturn(new AccountCrawlChunk(Arrays.asList(account2), ACCOUNT2));
    when(accounts.getAllFromDynamo(eq(ACCOUNT2), anyInt())).thenReturn(new AccountCrawlChunk(Collections.emptyList(), null));

    when(cache.claimActiveWork(any(), anyLong())).thenReturn(true);
    when(cache.isAccelerated()).thenReturn(false);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    dynamicAccountsDynamoDbMigrationConfiguration = mock(DynamicAccountsDynamoDbMigrationConfiguration.class);
    when(dynamicConfiguration.getAccountsDynamoDbMigrationConfiguration()).thenReturn(dynamicAccountsDynamoDbMigrationConfiguration);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCrawlStart(final boolean useDynamo) throws AccountDatabaseCrawlerRestartException {
    when(dynamicAccountsDynamoDbMigrationConfiguration.isDynamoCrawlerEnabled()).thenReturn(useDynamo);
    when(cache.getLastUuid()).thenReturn(Optional.empty());
    when(cache.getLastUuidDynamo()).thenReturn(Optional.empty());

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(useDynamo ? 0 : 1)).getLastUuid();
    verify(cache, times(useDynamo ? 1 : 0)).getLastUuidDynamo();
    verify(listener, times(1)).onCrawlStart();
    if (useDynamo) {
      verify(accounts, times(1)).getAllFromDynamo(eq(CHUNK_SIZE));
      verify(accounts, times(0)).getAllFromDynamo(any(UUID.class), eq(CHUNK_SIZE));
    } else {
      verify(accounts, times(1)).getAllFrom(eq(CHUNK_SIZE));
      verify(accounts, times(0)).getAllFrom(any(UUID.class), eq(CHUNK_SIZE));
    }
    verify(account1, times(0)).getUuid();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.empty()), eq(Arrays.asList(account1, account2)));
    verify(cache, times(useDynamo ? 0 : 1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(useDynamo ? 1 : 0)).setLastUuidDynamo(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyNoMoreInteractions(account1);
    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCrawlChunk(final boolean useDynamo) throws AccountDatabaseCrawlerRestartException {
    when(dynamicAccountsDynamoDbMigrationConfiguration.isDynamoCrawlerEnabled()).thenReturn(useDynamo);
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));
    when(cache.getLastUuidDynamo()).thenReturn(Optional.of(ACCOUNT1));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(useDynamo ? 0: 1)).getLastUuid();
    verify(cache, times(useDynamo ? 1: 0)).getLastUuidDynamo();
    if (useDynamo) {
      verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT1), eq(CHUNK_SIZE));
    } else {
      verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFrom(eq(ACCOUNT1), eq(CHUNK_SIZE));
    }
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));
    verify(cache, times(useDynamo ? 0 : 1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(useDynamo ? 1 : 0)).setLastUuidDynamo(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCrawlChunkAccelerated(final boolean useDynamo) throws AccountDatabaseCrawlerRestartException {
    when(dynamicAccountsDynamoDbMigrationConfiguration.isDynamoCrawlerEnabled()).thenReturn(useDynamo);
    when(cache.isAccelerated()).thenReturn(true);
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));
    when(cache.getLastUuidDynamo()).thenReturn(Optional.of(ACCOUNT1));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isTrue();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(useDynamo ? 0 : 1)).getLastUuid();
    verify(cache, times(useDynamo ? 1 : 0)).getLastUuidDynamo();
    if (useDynamo) {
      verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT1), eq(CHUNK_SIZE));
    } else {
      verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFrom(eq(ACCOUNT1), eq(CHUNK_SIZE));
    }
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));
    verify(cache, times(useDynamo ? 0 : 1)).setLastUuid(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(useDynamo ? 1 : 0)).setLastUuidDynamo(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCrawlChunkRestart(final boolean useDynamo) throws AccountDatabaseCrawlerRestartException {
    when(dynamicAccountsDynamoDbMigrationConfiguration.isDynamoCrawlerEnabled()).thenReturn(useDynamo);
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT1));
    when(cache.getLastUuidDynamo()).thenReturn(Optional.of(ACCOUNT1));
    doThrow(AccountDatabaseCrawlerRestartException.class).when(listener).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(useDynamo ? 0 : 1)).getLastUuid();
    verify(cache, times(useDynamo ? 1 : 0)).getLastUuidDynamo();
    if (useDynamo) {
      verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT1), eq(CHUNK_SIZE));
    } else {
      verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFrom(eq(ACCOUNT1), eq(CHUNK_SIZE));
    }
    verify(account2, times(0)).getNumber();
    verify(listener, times(1)).timeAndProcessCrawlChunk(eq(Optional.of(ACCOUNT1)), eq(Arrays.asList(account2)));
    verify(cache, times(useDynamo ? 0 : 1)).setLastUuid(eq(Optional.empty()));
    verify(cache, times(useDynamo ? 1 : 0)).setLastUuidDynamo(eq(Optional.empty()));
    verify(cache, times(1)).setAccelerated(false);
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);

    verifyNoMoreInteractions(account2);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCrawlEnd(final boolean useDynamo) {
    when(dynamicAccountsDynamoDbMigrationConfiguration.isDynamoCrawlerEnabled()).thenReturn(useDynamo);
    when(cache.getLastUuid()).thenReturn(Optional.of(ACCOUNT2));
    when(cache.getLastUuidDynamo()).thenReturn(Optional.of(ACCOUNT2));

    boolean accelerated = crawler.doPeriodicWork();
    assertThat(accelerated).isFalse();

    verify(cache, times(1)).claimActiveWork(any(String.class), anyLong());
    verify(cache, times(useDynamo ? 0 : 1)).getLastUuid();
    verify(cache, times(useDynamo ? 1 : 0)).getLastUuidDynamo();
    if (useDynamo) {
      verify(accounts, times(0)).getAllFromDynamo(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFromDynamo(eq(ACCOUNT2), eq(CHUNK_SIZE));
    } else {
      verify(accounts, times(0)).getAllFrom(eq(CHUNK_SIZE));
      verify(accounts, times(1)).getAllFrom(eq(ACCOUNT2), eq(CHUNK_SIZE));
    }
    verify(account1, times(0)).getNumber();
    verify(account2, times(0)).getNumber();
    verify(listener, times(1)).onCrawlEnd(eq(Optional.of(ACCOUNT2)));
    verify(cache, times(useDynamo ? 0 : 1)).setLastUuid(eq(Optional.empty()));
    verify(cache, times(useDynamo ? 1 : 0)).setLastUuidDynamo(eq(Optional.empty()));
    verify(cache, times(1)).setAccelerated(false);
    verify(cache, times(1)).isAccelerated();
    verify(cache, times(1)).releaseActiveWork(any(String.class));

    verifyZeroInteractions(account1);
    verifyZeroInteractions(account2);

    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(listener);
    verifyNoMoreInteractions(cache);
  }

}
