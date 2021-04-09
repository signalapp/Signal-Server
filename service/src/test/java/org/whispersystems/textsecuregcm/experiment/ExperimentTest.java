/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import io.micrometer.core.instrument.Timer;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@RunWith(JUnitParamsRunner.class)
public class ExperimentTest {

    private Timer matchTimer;
    private Timer errorTimer;

    private Experiment experiment;

    @Before
    public void setUp() {
        matchTimer = mock(Timer.class);
        errorTimer = mock(Timer.class);

        experiment = new Experiment("test", matchTimer, errorTimer, mock(Timer.class), mock(Timer.class), mock(Timer.class));
    }

    @Test
    public void compareFutureResult() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(12));
        verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
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
    public void compareSupplierResultError() {
        experiment.compareSupplierResult(12, () -> { throw new RuntimeException("OH NO"); });
        verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultAsyncMatch() throws InterruptedException {
        final ExecutorService experimentExecutor = Executors.newSingleThreadExecutor();

        experiment.compareSupplierResultAsync(12, () -> 12, experimentExecutor);
        experimentExecutor.shutdown();
        experimentExecutor.awaitTermination(1, TimeUnit.SECONDS);

        verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultAsyncError() throws InterruptedException {
        final ExecutorService experimentExecutor = Executors.newSingleThreadExecutor();

        experiment.compareSupplierResultAsync(12, () -> { throw new RuntimeException("OH NO"); }, experimentExecutor);
        experimentExecutor.shutdown();
        experimentExecutor.awaitTermination(1, TimeUnit.SECONDS);

        verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareSupplierResultAsyncRejection() {
        final ExecutorService executorService = mock(ExecutorService.class);
        doThrow(new RejectedExecutionException()).when(executorService).execute(any(Runnable.class));

        experiment.compareSupplierResultAsync(12, () -> 12, executorService);
        verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    @Parameters(method = "argumentsForTestRecordResult")
    public void testRecordResult(final Object expected, final Object actual, final Experiment experiment, final Timer expectedTimer) {
        reset(expectedTimer);

        final long durationNanos = 123;

        experiment.recordResult(expected, actual, durationNanos);
        verify(expectedTimer).record(durationNanos, TimeUnit.NANOSECONDS);
    }

    @SuppressWarnings("unused")
    private Object argumentsForTestRecordResult() {
        // Hack: parameters are set before the @Before method gets called
        final Timer matchTimer                  = mock(Timer.class);
        final Timer errorTimer                  = mock(Timer.class);
        final Timer bothPresentMismatchTimer    = mock(Timer.class);
        final Timer controlNullMismatchTimer    = mock(Timer.class);
        final Timer experimentNullMismatchTimer = mock(Timer.class);

        final Experiment experiment = new Experiment("test", matchTimer, errorTimer, bothPresentMismatchTimer, controlNullMismatchTimer, experimentNullMismatchTimer);

        return new Object[] {
                new Object[] { 12,               12,               experiment, matchTimer },
                new Object[] { null,             12,               experiment, controlNullMismatchTimer },
                new Object[] { 12,               null,             experiment, experimentNullMismatchTimer },
                new Object[] { 12,               17,               experiment, bothPresentMismatchTimer },
                new Object[] { Optional.of(12),  Optional.of(12),  experiment, matchTimer },
                new Object[] { Optional.empty(), Optional.of(12),  experiment, controlNullMismatchTimer },
                new Object[] { Optional.of(12),  Optional.empty(), experiment, experimentNullMismatchTimer },
                new Object[] { Optional.of(12),  Optional.of(17),  experiment, bothPresentMismatchTimer }
        };
    }
}
