/**
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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.ActiveUserCache;
import org.whispersystems.textsecuregcm.storage.ActiveUserCounter;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Util;

import io.dropwizard.metrics.MetricsFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ActiveUserCounterTest {
  private final Device iosDevice     = mock(Device.class);
  private final Device androidDevice = mock(Device.class);

  private final Account androidAccount = mock(Account.class);
  private final Account iosAccount     = mock(Account.class);
  private final Account invalidAccount = mock(Account.class);

  private final Accounts accounts = mock(Accounts.class);
  private final MetricsFactory metricsFactory = mock(MetricsFactory.class);

  private final ActiveUserCache activeUserCache = mock(ActiveUserCache.class);
  private final ActiveUserCounter activeUserCounter = new ActiveUserCounter(metricsFactory, accounts, activeUserCache);

  private long[] EMPTY_TALLIES = {0L,0L,0L,0L,0L};

  @Before
  public void setup() {

    long oneDayAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);

    when(androidDevice.getApnId()).thenReturn(null);
    when(androidDevice.getGcmId()).thenReturn("mock-gcm-id");
    when(androidDevice.getLastSeen()).thenReturn(oneDayAgo);

    when(iosDevice.getApnId()).thenReturn("mock-apn-id");
    when(iosDevice.getGcmId()).thenReturn(null);
    when(iosDevice.getLastSeen()).thenReturn(oneDayAgo);

    when(iosAccount.getNumber()).thenReturn("+1");
    when(iosAccount.getMasterDevice()).thenReturn(Optional.of(iosDevice));

    when(androidAccount.getNumber()).thenReturn("+2");
    when(androidAccount.getMasterDevice()).thenReturn(Optional.of(androidDevice));

    when(invalidAccount.getNumber()).thenReturn("+3");
    when(invalidAccount.getMasterDevice()).thenReturn(Optional.ofNullable(null));

    when(accounts.getAllFrom(eq("+"), anyInt())).thenReturn(Arrays.asList(iosAccount, androidAccount, invalidAccount));

    when(metricsFactory.getReporters()).thenReturn(ImmutableList.of());

    when(activeUserCache.getLastNumberVisited()).thenReturn(Optional.of("+"));
    when(activeUserCache.getDateToReport(anyInt())).thenReturn(20181101);
    when(activeUserCache.claimActiveWorker(any(), anyLong())).thenReturn(true);
    when(activeUserCache.getFinalTallies(any(), any())).thenReturn(EMPTY_TALLIES);
  }

  @Test
  public void testStart() {

    int     today      = activeUserCounter.getDateOfToday();
    boolean doMoreWork = activeUserCounter.doPeriodicWork(today);

    assertThat(doMoreWork).isTrue();

    verify(activeUserCache, times(1)).claimActiveWorker(any(), anyLong());
    verify(activeUserCache, times(1)).getLastNumberVisited();
    verify(activeUserCache, times(1)).getDateToReport(anyInt());

    verify(activeUserCache).setLastNumberVisited(eq(Optional.of("+")));
    verify(activeUserCache).setDateToReport(eq(today));
    verify(activeUserCache, times(1)).setDateToReport(anyInt());
    verify(activeUserCache, times(1)).resetTallies(any(), any());

    verify(accounts, times(1)).getAllFrom(any(), anyInt());

    verify(iosAccount, times(1)).getNumber();
    verify(iosAccount, times(1)).getMasterDevice();

    verify(androidAccount, times(1)).getNumber();
    verify(androidAccount, times(1)).getMasterDevice();

    verify(invalidAccount, times(1)).getNumber();
    verify(invalidAccount, times(1)).getMasterDevice();

    verify(iosDevice, times(1)).getLastSeen();
    verify(iosDevice, times(1)).getApnId();
    verify(iosDevice, times(0)).getGcmId();

    verify(androidDevice, times(1)).getApnId();
    verify(androidDevice, times(1)).getGcmId();
    verify(androidDevice, times(1)).getLastSeen();

    verify(activeUserCache,times(2)).incrementTallies(any(), any(), any());
    verify(activeUserCache).setLastNumberVisited(eq(Optional.of("+3")));
    verify(activeUserCache, times(2)).setLastNumberVisited(any());

    verify(activeUserCache, times(0)).getFinalTallies(any(), any());
    verify(metricsFactory, times(0)).getReporters();

    verify(activeUserCache, times(1)).releaseActiveWorker(any());

    verifyZeroInteractions(metricsFactory);

    verifyNoMoreInteractions(iosDevice);
    verifyNoMoreInteractions(androidDevice);
    verifyNoMoreInteractions(iosAccount);
    verifyNoMoreInteractions(androidAccount);
    verifyNoMoreInteractions(invalidAccount);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(activeUserCache);
  }

  @Test
  public void testInProgressValid() {

    int today = activeUserCounter.getDateOfToday();

    when(accounts.getAllFrom(eq("+"), anyInt())).thenReturn(Arrays.asList(iosAccount));
    when(activeUserCache.getDateToReport(anyInt())).thenReturn(today);

    boolean doMoreWork = activeUserCounter.doPeriodicWork(today);

    assertThat(doMoreWork).isTrue();

    verify(activeUserCache, times(1)).claimActiveWorker(any(), anyLong());
    verify(activeUserCache, times(1)).getLastNumberVisited();
    verify(activeUserCache, times(1)).getDateToReport(anyInt());

    verify(activeUserCache, times(0)).setDateToReport(anyInt());
    verify(activeUserCache, times(0)).resetTallies(any(), any());

    verify(accounts, times(1)).getAllFrom(any(), anyInt());
    verify(activeUserCache,times(2)).incrementTallies(any(), any(), any());

    verify(iosAccount, times(1)).getNumber();
    verify(iosAccount, times(1)).getMasterDevice();

    verify(iosDevice, times(1)).getLastSeen();
    verify(iosDevice, times(1)).getApnId();
    verify(iosDevice, times(0)).getGcmId();

    verify(activeUserCache,times(2)).incrementTallies(any(), any(), any());
    verify(activeUserCache).setLastNumberVisited(eq(Optional.of("+1")));
    verify(activeUserCache, times(1)).setLastNumberVisited(any());

    verify(activeUserCache, times(0)).getFinalTallies(any(), any());
    verify(metricsFactory, times(0)).getReporters();

    verify(activeUserCache, times(1)).releaseActiveWorker(any());

    verifyZeroInteractions(androidDevice);
    verifyZeroInteractions(androidAccount);
    verifyZeroInteractions(invalidAccount);

    verifyNoMoreInteractions(iosDevice);
    verifyNoMoreInteractions(iosAccount);
    verifyNoMoreInteractions(metricsFactory);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(activeUserCache);
  }

  @Test
  public void testInProgressInvalid() {

    int today = activeUserCounter.getDateOfToday();

    when(accounts.getAllFrom(eq("+"), anyInt())).thenReturn(Arrays.asList(invalidAccount));
    when(activeUserCache.getDateToReport(anyInt())).thenReturn(today);

    boolean doMoreWork = activeUserCounter.doPeriodicWork(today);

    assertThat(doMoreWork).isTrue();

    verify(activeUserCache, times(1)).claimActiveWorker(any(), anyLong());
    verify(activeUserCache, times(1)).getLastNumberVisited();
    verify(activeUserCache, times(1)).getDateToReport(anyInt());

    verify(activeUserCache, times(0)).setDateToReport(anyInt());
    verify(activeUserCache, times(0)).resetTallies(any(), any());

    verify(accounts, times(1)).getAllFrom(any(), anyInt());
    verify(activeUserCache,times(2)).incrementTallies(any(), any(), any());

    verify(invalidAccount, times(1)).getNumber();
    verify(invalidAccount, times(1)).getMasterDevice();

    verify(activeUserCache,times(2)).incrementTallies(any(), any(), any());
    verify(activeUserCache).setLastNumberVisited(eq(Optional.of("+3")));
    verify(activeUserCache, times(1)).setLastNumberVisited(any());

    verify(activeUserCache, times(0)).getFinalTallies(any(), any());
    verify(metricsFactory, times(0)).getReporters();

    verify(activeUserCache, times(1)).releaseActiveWorker(any());

    verifyZeroInteractions(iosDevice);
    verifyZeroInteractions(iosAccount);
    verifyZeroInteractions(androidDevice);
    verifyZeroInteractions(androidAccount);

    verifyNoMoreInteractions(invalidAccount);
    verifyNoMoreInteractions(metricsFactory);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(activeUserCache);
  }

  @Test
  public void testFinish() {

    int today = activeUserCounter.getDateOfToday();

    when(accounts.getAllFrom(eq("+"), anyInt())).thenReturn(Collections.emptyList());
    when(activeUserCache.getDateToReport(anyInt())).thenReturn(today);

    boolean doMoreWork = activeUserCounter.doPeriodicWork(today);

    assertThat(doMoreWork).isFalse();

    verify(activeUserCache, times(1)).claimActiveWorker(any(), anyLong());
    verify(activeUserCache, times(1)).getLastNumberVisited();
    verify(activeUserCache, times(1)).getDateToReport(anyInt());

    verify(activeUserCache, times(0)).setDateToReport(anyInt());
    verify(activeUserCache, times(0)).resetTallies(any(), any());

    verify(accounts, times(1)).getAllFrom(any(), anyInt());
    verify(activeUserCache,times(2)).incrementTallies(any(), any(), any());
    verify(activeUserCache).setLastNumberVisited(eq(Optional.ofNullable(null)));
    verify(activeUserCache, times(1)).setLastNumberVisited(any());

    verify(activeUserCache, times(2)).getFinalTallies(any(), any());
    verify(metricsFactory, times(1)).getReporters();

    verify(activeUserCache, times(1)).releaseActiveWorker(any());

    verifyZeroInteractions(iosDevice);
    verifyZeroInteractions(androidDevice);
    verifyZeroInteractions(iosAccount);
    verifyZeroInteractions(androidAccount);
    verifyZeroInteractions(invalidAccount);

    verifyNoMoreInteractions(metricsFactory);
    verifyNoMoreInteractions(accounts);
    verifyNoMoreInteractions(activeUserCache);}

}
