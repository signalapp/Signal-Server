/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.ActiveUserCounter;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerRestartException;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

import com.google.common.collect.ImmutableList;
import io.dropwizard.metrics.MetricsFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ActiveUserCounterTest {

  private final UUID UUID_IOS      = UUID.randomUUID();
  private final UUID UUID_ANDROID  = UUID.randomUUID();
  private final UUID UUID_NODEVICE = UUID.randomUUID();

  private final String ACCOUNT_NUMBER_IOS      = "+15551234567";
  private final String ACCOUNT_NUMBER_ANDROID  = "+5511987654321";
  private final String ACCOUNT_NUMBER_NODEVICE = "+5215551234567";

  private final String TALLY_KEY       = "active_user_tally";

  private final Device iosDevice     = mock(Device.class);
  private final Device androidDevice = mock(Device.class);

  private final Account androidAccount = mock(Account.class);
  private final Account iosAccount     = mock(Account.class);
  private final Account noDeviceAccount = mock(Account.class);

  private final RedisAdvancedClusterCommands<String, String> commands       = mock(RedisAdvancedClusterCommands.class);
  private final FaultTolerantRedisCluster                    cacheCluster   = RedisClusterHelper.buildMockRedisCluster(commands);
  private final MetricsFactory                               metricsFactory = mock(MetricsFactory.class);

  private final ActiveUserCounter activeUserCounter = new ActiveUserCounter(metricsFactory, cacheCluster);

  @Before
  public void setup() {

    long halfDayAgo      = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(12);
    long fortyFiveDayAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(45);

    when(androidDevice.getApnId()).thenReturn(null);
    when(androidDevice.getGcmId()).thenReturn("mock-gcm-id");
    when(androidDevice.getLastSeen()).thenReturn(fortyFiveDayAgo);

    when(iosDevice.getApnId()).thenReturn("mock-apn-id");
    when(iosDevice.getGcmId()).thenReturn(null);
    when(iosDevice.getLastSeen()).thenReturn(halfDayAgo);

    when(iosAccount.getUuid()).thenReturn(UUID_IOS);
    when(iosAccount.getMasterDevice()).thenReturn(Optional.of(iosDevice));
    when(iosAccount.getNumber()).thenReturn(ACCOUNT_NUMBER_IOS);

    when(androidAccount.getUuid()).thenReturn(UUID_ANDROID);
    when(androidAccount.getMasterDevice()).thenReturn(Optional.of(androidDevice));
    when(androidAccount.getNumber()).thenReturn(ACCOUNT_NUMBER_ANDROID);

    when(noDeviceAccount.getUuid()).thenReturn(UUID_NODEVICE);
    when(noDeviceAccount.getMasterDevice()).thenReturn(Optional.ofNullable(null));
    when(noDeviceAccount.getNumber()).thenReturn(ACCOUNT_NUMBER_NODEVICE);

    when(commands.get(any(String.class))).thenReturn("{\"fromNumber\":\"+\",\"platforms\":{},\"countries\":{}}");
    when(metricsFactory.getReporters()).thenReturn(ImmutableList.of());
  }

  @Test
  public void testCrawlStart() {
    activeUserCounter.onCrawlStart();

    verify(cacheCluster, times(1)).useCluster(any());
    verify(commands, times(1)).del(any(String.class));

    verifyZeroInteractions(iosDevice);
    verifyZeroInteractions(iosAccount);
    verifyZeroInteractions(androidDevice);
    verifyZeroInteractions(androidAccount);
    verifyZeroInteractions(noDeviceAccount);
    verifyZeroInteractions(metricsFactory);
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(cacheCluster);
  }

  @Test
  public void testCrawlEnd() {
    activeUserCounter.onCrawlEnd(Optional.empty());

    verify(cacheCluster, times(1)).withCluster(any());
    verify(commands, times(1)).get(any(String.class));

    verify(metricsFactory, times(1)).getReporters();

    verifyZeroInteractions(iosDevice);
    verifyZeroInteractions(iosAccount);
    verifyZeroInteractions(androidDevice);
    verifyZeroInteractions(androidAccount);
    verifyZeroInteractions(noDeviceAccount);

    verifyNoMoreInteractions(metricsFactory);
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(cacheCluster);

  }

  @Test
  public void testCrawlChunkValidAccount() throws AccountDatabaseCrawlerRestartException {
    activeUserCounter.timeAndProcessCrawlChunk(Optional.of(UUID_IOS), Arrays.asList(iosAccount));

    verify(iosAccount, times(1)).getMasterDevice();
    verify(iosAccount, times(1)).getNumber();

    verify(iosDevice, times(1)).getLastSeen();
    verify(iosDevice, times(1)).getApnId();
    verify(iosDevice, times(0)).getGcmId();

    verify(cacheCluster, times(1)).withCluster(any());
    verify(cacheCluster, times(1)).useCluster(any());
    verify(commands, times(1)).get(any(String.class));
    verify(commands, times(1)).set(any(String.class), eq("{\"fromUuid\":\""+UUID_IOS.toString()+"\",\"platforms\":{\"ios\":[1,1,1,1,1]},\"countries\":{\"1\":[1,1,1,1,1]}}"));

    verify(metricsFactory, times(0)).getReporters();

    verifyZeroInteractions(androidDevice);
    verifyZeroInteractions(androidAccount);
    verifyZeroInteractions(noDeviceAccount);
    verifyZeroInteractions(metricsFactory);

    verifyNoMoreInteractions(iosDevice);
    verifyNoMoreInteractions(iosAccount);
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(cacheCluster);
  }

  @Test
  public void testCrawlChunkNoDeviceAccount() throws AccountDatabaseCrawlerRestartException {
    activeUserCounter.timeAndProcessCrawlChunk(Optional.of(UUID_NODEVICE), Arrays.asList(noDeviceAccount));

    verify(noDeviceAccount, times(1)).getMasterDevice();

    verify(cacheCluster, times(1)).withCluster(any());
    verify(cacheCluster, times(1)).useCluster(any());
    verify(commands, times(1)).get(eq(TALLY_KEY));
    verify(commands, times(1)).set(any(String.class), eq("{\"fromUuid\":\""+UUID_NODEVICE+"\",\"platforms\":{},\"countries\":{}}"));

    verify(metricsFactory, times(0)).getReporters();

    verifyZeroInteractions(iosDevice);
    verifyZeroInteractions(iosAccount);
    verifyZeroInteractions(androidDevice);
    verifyZeroInteractions(androidAccount);
    verifyZeroInteractions(noDeviceAccount);
    verifyZeroInteractions(metricsFactory);

    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(cacheCluster);
  }

  @Test
  public void testCrawlChunkMixedAccount() throws AccountDatabaseCrawlerRestartException {
    activeUserCounter.timeAndProcessCrawlChunk(Optional.of(UUID_IOS), Arrays.asList(iosAccount, androidAccount, noDeviceAccount));

    verify(iosAccount, times(1)).getMasterDevice();
    verify(iosAccount, times(1)).getNumber();
    verify(androidAccount, times(1)).getMasterDevice();
    verify(androidAccount, times(1)).getNumber();
    verify(noDeviceAccount, times(1)).getMasterDevice();

    verify(iosDevice, times(1)).getLastSeen();
    verify(iosDevice, times(1)).getApnId();
    verify(iosDevice, times(0)).getGcmId();

    verify(androidDevice, times(1)).getLastSeen();
    verify(androidDevice, times(1)).getApnId();
    verify(androidDevice, times(1)).getGcmId();

    verify(cacheCluster, times(1)).withCluster(any());
    verify(cacheCluster, times(1)).useCluster(any());
    verify(commands, times(1)).get(eq(TALLY_KEY));
    verify(commands, times(1)).set(any(String.class), eq("{\"fromUuid\":\""+UUID_IOS+"\",\"platforms\":{\"android\":[0,0,0,1,1],\"ios\":[1,1,1,1,1]},\"countries\":{\"55\":[0,0,0,1,1],\"1\":[1,1,1,1,1]}}"));

    verify(metricsFactory, times(0)).getReporters();

    verifyZeroInteractions(metricsFactory);

    verifyNoMoreInteractions(iosDevice);
    verifyNoMoreInteractions(iosAccount);
    verifyNoMoreInteractions(androidDevice);
    verifyNoMoreInteractions(androidAccount);
    verifyNoMoreInteractions(noDeviceAccount);
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(cacheCluster);
  }

}
