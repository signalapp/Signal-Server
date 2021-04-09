/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.ReporterFactory;
import io.lettuce.core.KeyValue;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RegistrationLockVersionCounterTest {

    private RedisAdvancedClusterCommands<String, String> redisCommands;
    private MetricsFactory                               metricsFactory;

    private RegistrationLockVersionCounter registrationLockVersionCounter;

    @Before
    public void setUp() {
        //noinspection unchecked
        redisCommands  = mock(RedisAdvancedClusterCommands.class);
        metricsFactory = mock(MetricsFactory.class);

        registrationLockVersionCounter = new RegistrationLockVersionCounter(RedisClusterHelper.buildMockRedisCluster(redisCommands), metricsFactory);
    }

    @Test
    public void testOnCrawlChunkNoReglock() {
        final Account                account          = mock(Account.class);
        final StoredRegistrationLock registrationLock = mock(StoredRegistrationLock.class);

        when(account.getRegistrationLock()).thenReturn(registrationLock);
        when(registrationLock.hasDeprecatedPin()).thenReturn(false);
        when(registrationLock.needsFailureCredentials()).thenReturn(false);

        registrationLockVersionCounter.onCrawlChunk(Optional.empty(), List.of(account));

        verifyCount(0, 0);
    }

    @Test
    public void testOnCrawlChunkPin() {
        final Account                account          = mock(Account.class);
        final StoredRegistrationLock registrationLock = mock(StoredRegistrationLock.class);

        when(account.getRegistrationLock()).thenReturn(registrationLock);
        when(registrationLock.requiresClientRegistrationLock()).thenReturn(true);
        when(registrationLock.hasDeprecatedPin()).thenReturn(true);

        registrationLockVersionCounter.onCrawlChunk(Optional.empty(), List.of(account));

        verifyCount(1, 0);
    }

    @Test
    public void testOnCrawlChunkReglock() {
        final Account                account          = mock(Account.class);
        final StoredRegistrationLock registrationLock = mock(StoredRegistrationLock.class);

        when(account.getRegistrationLock()).thenReturn(registrationLock);
        when(registrationLock.requiresClientRegistrationLock()).thenReturn(true);
        when(registrationLock.hasDeprecatedPin()).thenReturn(false);

        registrationLockVersionCounter.onCrawlChunk(Optional.empty(), List.of(account));

        verifyCount(0, 1);
    }

    private void verifyCount(final int pinCount, final int reglockCount) {
        verify(redisCommands).hincrby(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY, RegistrationLockVersionCounter.PIN_KEY,     pinCount);
        verify(redisCommands).hincrby(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY, RegistrationLockVersionCounter.REGLOCK_KEY, reglockCount);
    }

    @Test
    public void testOnCrawlEnd() {
        final int pinCount     = 7;
        final int reglockCount = 83;

        final ReporterFactory   reporterFactory = mock(ReporterFactory.class);
        final ScheduledReporter reporter        = mock(ScheduledReporter.class);

        when(metricsFactory.getReporters()).thenReturn(List.of(reporterFactory));

        final ArgumentCaptor<MetricRegistry> registryCaptor = ArgumentCaptor.forClass(MetricRegistry.class);
        when(reporterFactory.build(any())).thenReturn(reporter);

        when(redisCommands.hmget(eq(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY), any())).thenReturn(List.of(
                KeyValue.just(RegistrationLockVersionCounter.PIN_KEY, String.valueOf(pinCount)),
                KeyValue.just(RegistrationLockVersionCounter.REGLOCK_KEY, String.valueOf(reglockCount))));

        registrationLockVersionCounter.onCrawlEnd(Optional.empty());

        verify(reporterFactory).build(registryCaptor.capture());
        verify(reporter).report();

        @SuppressWarnings("rawtypes") final Map<String, Gauge> gauges = registryCaptor.getValue().getGauges();
        assertEquals(pinCount,     gauges.get(name(RegistrationLockVersionCounter.class, RegistrationLockVersionCounter.PIN_KEY)).getValue());
        assertEquals(reglockCount, gauges.get(name(RegistrationLockVersionCounter.class, RegistrationLockVersionCounter.REGLOCK_KEY)).getValue());
    }
}
