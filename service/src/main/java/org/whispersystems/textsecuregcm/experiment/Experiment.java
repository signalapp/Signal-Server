package org.whispersystems.textsecuregcm.experiment;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * An experiment compares the results of two operations and records metrics to assess how frequently they match.
 */
public class Experiment {

    private final String name;

    private final Counter matchCounter;
    private final Counter errorCounter;

    private final Counter bothPresentMismatchCounter;
    private final Counter controlNullMismatchCounter;
    private final Counter experimentNullMismatchCounter;

    private static final String OUTCOME_TAG      = "outcome";
    private static final String MATCH_OUTCOME    = "match";
    private static final String MISMATCH_OUTCOME = "mismatch";
    private static final String ERROR_OUTCOME    = "error";

    private static final String MISMATCH_TYPE_TAG        = "mismatchType";
    private static final String BOTH_PRESENT_MISMATCH    = "bothPresent";
    private static final String CONTROL_NULL_MISMATCH    = "controlResultNull";
    private static final String EXPERIMENT_NULL_MISMATCH = "experimentResultNull";

    private static final Logger log = LoggerFactory.getLogger(Experiment.class);

    public Experiment(final String... names) {
        this(name(Experiment.class, names),
             Metrics.counter(name(Experiment.class, names), OUTCOME_TAG, MATCH_OUTCOME),
             Metrics.counter(name(Experiment.class, names), OUTCOME_TAG, ERROR_OUTCOME),
             Metrics.counter(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG, BOTH_PRESENT_MISMATCH),
             Metrics.counter(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG, CONTROL_NULL_MISMATCH),
             Metrics.counter(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG, EXPERIMENT_NULL_MISMATCH));
    }

    @VisibleForTesting
    Experiment(final String name, final Counter matchCounter, final Counter errorCounter, final Counter bothPresentMismatchCounter, final Counter controlNullMismatchCounter, final Counter experimentNullMismatchCounter) {
        this.name = name;

        this.matchCounter = matchCounter;
        this.errorCounter = errorCounter;

        this.bothPresentMismatchCounter = bothPresentMismatchCounter;
        this.controlNullMismatchCounter = controlNullMismatchCounter;
        this.experimentNullMismatchCounter = experimentNullMismatchCounter;
    }

    public <T> void compareFutureResult(final T expected, final CompletionStage<T> experimentStage) {
        experimentStage.whenComplete((actual, cause) -> {
            if (cause != null) {
                recordError(cause);
            } else {
                recordResult(expected, actual);
            }
        });
    }

    public <T> void compareSupplierResult(final T expected, final Supplier<T> experimentSupplier) {
        try {
            recordResult(expected, experimentSupplier.get());
        } catch (final Exception e) {
            recordError(e);
        }
    }

    private void recordError(final Throwable cause) {
        log.warn("Experiment {} threw an exception.", name, cause);
        errorCounter.increment();
    }

    private <T> void recordResult(final T expected, final T actual) {
        final Counter counter;

        if (Objects.equals(expected, actual)) {
            counter = matchCounter;
        } else if (expected == null) {
            counter = controlNullMismatchCounter;
        } else if (actual == null) {
            counter = experimentNullMismatchCounter;
        } else {
            counter = bothPresentMismatchCounter;
        }

        counter.increment();
    }
}
