package org.whispersystems.textsecuregcm.experiment;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import org.LatencyUtils.PauseDetector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ExperimentTest {

    private Timer matchTimer;
    private Timer mismatchTimer;
    private Timer errorTimer;

    private Experiment experiment;

    @Before
    public void setUp() {
        matchTimer = mock(Timer.class);
        mismatchTimer = mock(Timer.class);
        errorTimer = mock(Timer.class);

        experiment = new Experiment(matchTimer, mismatchTimer, errorTimer);
    }

    @Test
    public void compareFutureResultMatch() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(12));
        verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareFutureResultMismatch() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(77));
        verify(mismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareFutureResultError() {
        experiment.compareFutureResult(12, CompletableFuture.failedFuture(new RuntimeException("OH NO")));
        verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareFutureResultNoExperimentData() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(null));
        verifyNoMoreInteractions(matchTimer, mismatchTimer, errorTimer);
    }

    @Test
    public void compareSupplierResultMatch() {
        experiment.compareSupplierResult(12, () -> 12);
        verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultMismatch() {
        experiment.compareSupplierResult(12, () -> 77);
        verify(mismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultError() {
        experiment.compareSupplierResult(12, () -> { throw new RuntimeException("OH NO"); });
        verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultNoExperimentData() {
        experiment.compareSupplierResult(12, () -> null);
        verifyNoMoreInteractions(mismatchTimer, mismatchTimer, errorTimer);
    }
}
