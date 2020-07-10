package org.whispersystems.textsecuregcm.experiment;

import io.micrometer.core.instrument.Counter;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class ExperimentTest {

    private Counter matchCounter;
    private Counter errorCounter;

    private Experiment experiment;

    @Before
    public void setUp() {
        matchCounter = mock(Counter.class);
        errorCounter = mock(Counter.class);

        experiment = new Experiment("test", matchCounter, errorCounter, mock(Counter.class), mock(Counter.class), mock(Counter.class));
    }

    @Test
    public void compareFutureResult() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(12));
        verify(matchCounter).increment();
    }

    @Test
    public void compareFutureResultError() {
        experiment.compareFutureResult(12, CompletableFuture.failedFuture(new RuntimeException("OH NO")));
        verify(errorCounter).increment();
    }

    @Test
    public void compareSupplierResultMatch() {
        experiment.compareSupplierResult(12, () -> 12);
        verify(matchCounter).increment();
    }

    @Test
    public void compareSupplierResultError() {
        experiment.compareSupplierResult(12, () -> { throw new RuntimeException("OH NO"); });
        verify(errorCounter).increment();
    }

    @Test
    public void compareSupplierResultAsyncMatch() throws InterruptedException {
        final ExecutorService experimentExecutor = Executors.newSingleThreadExecutor();

        experiment.compareSupplierResultAsync(12, () -> 12, experimentExecutor);
        experimentExecutor.shutdown();
        experimentExecutor.awaitTermination(1, TimeUnit.SECONDS);

        verify(matchCounter).increment();
    }

    @Test
    public void compareSupplierResultAsyncError() throws InterruptedException {
        final ExecutorService experimentExecutor = Executors.newSingleThreadExecutor();

        experiment.compareSupplierResultAsync(12, () -> { throw new RuntimeException("OH NO"); }, experimentExecutor);
        experimentExecutor.shutdown();
        experimentExecutor.awaitTermination(1, TimeUnit.SECONDS);

        verify(errorCounter).increment();
    }

    @Test
    public void compareSupplierResultAsyncRejection() {
        final ExecutorService executorService = mock(ExecutorService.class);
        doThrow(new RejectedExecutionException()).when(executorService).execute(any(Runnable.class));

        experiment.compareSupplierResultAsync(12, () -> 12, executorService);
        verify(errorCounter).increment();
    }

    @Test
    @Parameters(method = "argumentsForTestRecordResult")
    public void testRecordResult(final Object expected, final Object actual, final Experiment experiment, final Counter expectedCounter) {
        reset(expectedCounter);

        experiment.recordResult(expected, actual);
        verify(expectedCounter).increment();
    }

    @SuppressWarnings("unused")
    private Object argumentsForTestRecordResult() {
        // Hack: parameters are set before the @Before method gets called
        final Counter matchCounter = mock(Counter.class);
        final Counter errorCounter = mock(Counter.class);
        final Counter bothPresentMismatchCounter = mock(Counter.class);
        final Counter controlNullMismatchCounter = mock(Counter.class);
        final Counter experimentNullMismatchCounter = mock(Counter.class);

        final Experiment experiment = new Experiment("test", matchCounter, errorCounter, bothPresentMismatchCounter, controlNullMismatchCounter, experimentNullMismatchCounter);

        return new Object[] {
                new Object[] { 12,               12,               experiment, matchCounter },
                new Object[] { null,             12,               experiment, controlNullMismatchCounter },
                new Object[] { 12,               null,             experiment, experimentNullMismatchCounter },
                new Object[] { 12,               17,               experiment, bothPresentMismatchCounter },
                new Object[] { Optional.of(12),  Optional.of(12),  experiment, matchCounter },
                new Object[] { Optional.empty(), Optional.of(12),  experiment, controlNullMismatchCounter },
                new Object[] { Optional.of(12),  Optional.empty(), experiment, experimentNullMismatchCounter },
                new Object[] { Optional.of(12),  Optional.of(17),  experiment, bothPresentMismatchCounter }
        };
    }
}
