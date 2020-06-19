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

        verifyCount(1, 0, 0, 0);
    }

    @Test
    public void testOnCrawlChunkPinOnly() {
        final Account                account          = mock(Account.class);
        final StoredRegistrationLock registrationLock = mock(StoredRegistrationLock.class);

        when(account.getRegistrationLock()).thenReturn(registrationLock);
        when(registrationLock.hasDeprecatedPin()).thenReturn(true);
        when(registrationLock.needsFailureCredentials()).thenReturn(false);

        registrationLockVersionCounter.onCrawlChunk(Optional.empty(), List.of(account));

        verifyCount(0, 1, 0, 0);
    }

    @Test
    public void testOnCrawlChunkReglockOnly() {
        final Account                account          = mock(Account.class);
        final StoredRegistrationLock registrationLock = mock(StoredRegistrationLock.class);

        when(account.getRegistrationLock()).thenReturn(registrationLock);
        when(registrationLock.hasDeprecatedPin()).thenReturn(false);
        when(registrationLock.needsFailureCredentials()).thenReturn(true);

        registrationLockVersionCounter.onCrawlChunk(Optional.empty(), List.of(account));

        verifyCount(0, 0, 1, 0);
    }

    @Test
    public void testOnCrawlChunkBoth() {
        final Account                account          = mock(Account.class);
        final StoredRegistrationLock registrationLock = mock(StoredRegistrationLock.class);

        when(account.getRegistrationLock()).thenReturn(registrationLock);
        when(registrationLock.hasDeprecatedPin()).thenReturn(true);
        when(registrationLock.needsFailureCredentials()).thenReturn(true);

        registrationLockVersionCounter.onCrawlChunk(Optional.empty(), List.of(account));

        verifyCount(0, 0, 0, 1);
    }

    private void verifyCount(final int noReglock, final int pinOnly, final int reglockOnly, final int both) {
        verify(redisCommands).hincrby(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY, RegistrationLockVersionCounter.NO_REGLOCK_KEY,   noReglock);
        verify(redisCommands).hincrby(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY, RegistrationLockVersionCounter.PIN_ONLY_KEY,     pinOnly);
        verify(redisCommands).hincrby(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY, RegistrationLockVersionCounter.REGLOCK_ONLY_KEY, reglockOnly);
        verify(redisCommands).hincrby(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY, RegistrationLockVersionCounter.BOTH_KEY,         both);
    }

    @Test
    public void testOnCrawlEnd() {
        final int noReglockCount   = 21;
        final int pinOnlyCount     = 7;
        final int reglockOnlyCount = 83;

        final ReporterFactory   reporterFactory = mock(ReporterFactory.class);
        final ScheduledReporter reporter        = mock(ScheduledReporter.class);

        when(metricsFactory.getReporters()).thenReturn(List.of(reporterFactory));

        final ArgumentCaptor<MetricRegistry> registryCaptor = ArgumentCaptor.forClass(MetricRegistry.class);
        when(reporterFactory.build(any())).thenReturn(reporter);

        when(redisCommands.hmget(eq(RegistrationLockVersionCounter.REGLOCK_COUNT_KEY), any())).thenReturn(List.of(
                KeyValue.just(RegistrationLockVersionCounter.NO_REGLOCK_KEY, String.valueOf(noReglockCount)),
                KeyValue.just(RegistrationLockVersionCounter.PIN_ONLY_KEY, String.valueOf(pinOnlyCount)),
                KeyValue.just(RegistrationLockVersionCounter.REGLOCK_ONLY_KEY, String.valueOf(reglockOnlyCount)),
                KeyValue.empty(RegistrationLockVersionCounter.BOTH_KEY)));

        registrationLockVersionCounter.onCrawlEnd(Optional.empty());

        verify(reporterFactory).build(registryCaptor.capture());
        verify(reporter).report();

        @SuppressWarnings("rawtypes") final Map<String, Gauge> gauges = registryCaptor.getValue().getGauges();
        assertEquals(noReglockCount, gauges.get(name(RegistrationLockVersionCounter.class, RegistrationLockVersionCounter.NO_REGLOCK_KEY)).getValue());
        assertEquals(pinOnlyCount, gauges.get(name(RegistrationLockVersionCounter.class, RegistrationLockVersionCounter.PIN_ONLY_KEY)).getValue());
        assertEquals(reglockOnlyCount, gauges.get(name(RegistrationLockVersionCounter.class, RegistrationLockVersionCounter.REGLOCK_ONLY_KEY)).getValue());
        assertEquals(0, gauges.get(name(RegistrationLockVersionCounter.class, RegistrationLockVersionCounter.BOTH_KEY)).getValue());
    }
}
