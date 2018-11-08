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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.dropwizard.metrics.ConsoleReporterFactory;
import io.dropwizard.metrics.MetricsFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.ActiveUserCache;
import org.whispersystems.textsecuregcm.storage.ActiveUserCounter;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ActiveUserCounterTest {
  private final Account  account  = mock(Account.class);
  private final Accounts accounts = mock(Accounts.class);

  private final WhisperServerConfiguration configuration = mock(WhisperServerConfiguration.class);
  private final MetricsFactory metricsFactory = mock(MetricsFactory.class);
  private final ConsoleReporterFactory consoleReporterFactory = new ConsoleReporterFactory();
  private final ActiveUserCache activeUserCache = mock(ActiveUserCache.class);
  private final ActiveUserCounter activeUserCounter = new ActiveUserCounter(configuration, accounts, activeUserCache);

  private long[] EMPTY_TALLIES = {0L,0L,0L,0L,0L};

  @Before
  public void setup() {
    when(configuration.getMetricsFactory()).thenReturn(metricsFactory);
    when(metricsFactory.getReporters()).thenReturn(ImmutableList.of(consoleReporterFactory));
    when(accounts.getActiveUsersFrom(anyLong(), anyInt())).thenReturn(Collections.emptyList());
    when(activeUserCache.getId()).thenReturn(Optional.of(0L));
    when(activeUserCache.getDate()).thenReturn(20181101);
    when(activeUserCache.claimActiveWorker(any(), anyLong())).thenReturn(true);
    when(activeUserCache.getFinalTallies(any(), any())).thenReturn(EMPTY_TALLIES);
  }

  @Test
  public void test() {
    //int today = activeUserCounter.getDateOfToday();
    //activeUserCounter.doPeriodicWork(today);
  }
}

