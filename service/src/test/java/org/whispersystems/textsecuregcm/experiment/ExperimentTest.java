package org.whispersystems.textsecuregcm.experiment;

import io.micrometer.core.instrument.Timer;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExperimentTest {

    private Timer matchTimer;
    private Timer errorTimer;
    private Timer bothPresentMismatchTimer;
    private Timer controlNullMismatchTimer;
    private Timer experimentNullMismatchTimer;

    private Experiment experiment;

    @Before
    public void setUp() {
        matchTimer = mock(Timer.class);
        errorTimer = mock(Timer.class);
        bothPresentMismatchTimer = mock(Timer.class);
        controlNullMismatchTimer = mock(Timer.class);
        experimentNullMismatchTimer = mock(Timer.class);

        experiment = new Experiment(matchTimer, errorTimer, bothPresentMismatchTimer, controlNullMismatchTimer, experimentNullMismatchTimer);
    }

    @Test
    public void compareFutureResultMatch() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(12));
        verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareFutureResultMismatchBothPresent() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(77));
        verify(bothPresentMismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareFutureResultMismatchControlNull() {
        experiment.compareFutureResult(null, CompletableFuture.completedFuture(77));
        verify(controlNullMismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareFutureResultMismatchExperimentNull() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(null));
        verify(experimentNullMismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareFutureResultError() {
        experiment.compareFutureResult(12, CompletableFuture.failedFuture(new RuntimeException("OH NO")));
        verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultMatch() {
        experiment.compareSupplierResult(12, () -> 12);
        verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultMismatchBothPresent() {
        experiment.compareSupplierResult(12, () -> 77);
        verify(bothPresentMismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultMismatchControlNull() {
        experiment.compareSupplierResult(null, () -> 77);
        verify(controlNullMismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultMismatchExperimentNull() {
        experiment.compareSupplierResult(12, () -> null);
        verify(experimentNullMismatchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultError() {
        experiment.compareSupplierResult(12, () -> { throw new RuntimeException("OH NO"); });
        verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }
}
