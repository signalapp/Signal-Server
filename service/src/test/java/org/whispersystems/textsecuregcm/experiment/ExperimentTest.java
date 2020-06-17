package org.whispersystems.textsecuregcm.experiment;

import io.micrometer.core.instrument.Counter;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExperimentTest {

    private Counter matchCounter;
    private Counter errorCounter;
    private Counter bothPresentMismatchCounter;
    private Counter controlNullMismatchCounter;
    private Counter experimentNullMismatchCounter;

    private Experiment experiment;

    @Before
    public void setUp() {
        matchCounter = mock(Counter.class);
        errorCounter = mock(Counter.class);
        bothPresentMismatchCounter = mock(Counter.class);
        controlNullMismatchCounter = mock(Counter.class);
        experimentNullMismatchCounter = mock(Counter.class);

        experiment = new Experiment(matchCounter, errorCounter, bothPresentMismatchCounter, controlNullMismatchCounter, experimentNullMismatchCounter);
    }

    @Test
    public void compareFutureResultMatch() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(12));
        verify(matchCounter).increment();
    }

    @Test
    public void compareFutureResultMismatchBothPresent() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(77));
        verify(bothPresentMismatchCounter).increment();
    }

    @Test
    public void compareFutureResultMismatchControlNull() {
        experiment.compareFutureResult(null, CompletableFuture.completedFuture(77));
        verify(controlNullMismatchCounter).increment();
    }

    @Test
    public void compareFutureResultMismatchExperimentNull() {
        experiment.compareFutureResult(12, CompletableFuture.completedFuture(null));
        verify(experimentNullMismatchCounter).increment();
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
    public void compareSupplierResultMismatchBothPresent() {
        experiment.compareSupplierResult(12, () -> 77);
        verify(bothPresentMismatchCounter).increment();
    }

    @Test
    public void compareSupplierResultMismatchControlNull() {
        experiment.compareSupplierResult(null, () -> 77);
        verify(controlNullMismatchCounter).increment();
    }

    @Test
    public void compareSupplierResultMismatchExperimentNull() {
        experiment.compareSupplierResult(12, () -> null);
        verify(experimentNullMismatchCounter).increment();
    }

    @Test
    public void compareSupplierResultError() {
        experiment.compareSupplierResult(12, () -> { throw new RuntimeException("OH NO"); });
        verify(errorCounter).increment();
    }
}
