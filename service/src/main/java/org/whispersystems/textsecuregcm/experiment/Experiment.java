package org.whispersystems.textsecuregcm.experiment;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An experiment compares the results of two operations and records metrics to assess how frequently they match.
 */
public class Experiment {

    private final Timer matchTimer;
    private final Timer errorTimer;

    private final Timer bothPresentMismatchTimer;
    private final Timer controlNullMismatchTimer;
    private final Timer experimentNullMismatchTimer;

    private static final String OUTCOME_TAG      = "outcome";
    private static final String MATCH_OUTCOME    = "match";
    private static final String MISMATCH_OUTCOME = "mismatch";
    private static final String ERROR_OUTCOME    = "error";

    private static final String MISMATCH_TYPE_TAG        = "mismatchType";
    private static final String BOTH_PRESENT_MISMATCH    = "bothPresent";
    private static final String CONTROL_NULL_MISMATCH    = "controlResultNull";
    private static final String EXPERIMENT_NULL_MISMATCH = "experimentResultNull";

    public Experiment(final String... names) {
        this(buildTimer(MATCH_OUTCOME, names).register(Metrics.globalRegistry),
             buildTimer(ERROR_OUTCOME, names).register(Metrics.globalRegistry),
             buildTimer(MISMATCH_OUTCOME, names).tag(MISMATCH_TYPE_TAG, BOTH_PRESENT_MISMATCH).register(Metrics.globalRegistry),
             buildTimer(MISMATCH_OUTCOME, names).tag(MISMATCH_TYPE_TAG, CONTROL_NULL_MISMATCH).register(Metrics.globalRegistry),
             buildTimer(MISMATCH_OUTCOME, names).tag(MISMATCH_TYPE_TAG, EXPERIMENT_NULL_MISMATCH).register(Metrics.globalRegistry));
    }

    private static Timer.Builder buildTimer(final String outcome, final String... names) {
        if (names == null || names.length == 0) {
            throw new IllegalArgumentException("Experiments must have a name");
        }

        return Timer.builder(MetricRegistry.name(Experiment.class, names))
                .tag(OUTCOME_TAG, outcome)
                .publishPercentileHistogram();
    }

    @VisibleForTesting
    Experiment(final Timer matchTimer, final Timer errorTimer, final Timer bothPresentMismatchTimer, final Timer controlNullMismatchTimer, final Timer experimentNullMismatchTimer) {
        this.matchTimer = matchTimer;
        this.errorTimer = errorTimer;

        this.bothPresentMismatchTimer = bothPresentMismatchTimer;
        this.controlNullMismatchTimer = controlNullMismatchTimer;
        this.experimentNullMismatchTimer = experimentNullMismatchTimer;
    }

    public <T> void compareFutureResult(final T expected, final CompletionStage<T> experimentStage) {
        // We're assuming that we get the experiment completion stage as soon as possible after it's started, but this
        // start time will inescapably have some "wiggle" to it.
        final long start = System.nanoTime();

        experimentStage.whenComplete((actual, cause) -> {
            final long duration = System.nanoTime() - start;

            if (cause != null) {
                recordError(duration);
            } else {
                recordResult(duration, expected, actual);
            }
        });
    }

    public <T> void compareSupplierResult(final T expected, final Supplier<T> experimentSupplier) {
        final long start = System.nanoTime();

        try {
            final T result = experimentSupplier.get();
            recordResult(System.nanoTime() - start, expected, result);
        } catch (final Exception e) {
            recordError(System.nanoTime() - start);
        }
    }

    private void recordError(final long durationNanos) {
        errorTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    private <T> void recordResult(final long durationNanos, final T expected, final T actual) {
        final Timer timer;

        if (Objects.equals(expected, actual)) {
            timer = matchTimer;
        } else if (expected == null) {
            timer = controlNullMismatchTimer;
        } else if (actual == null) {
            timer = experimentNullMismatchTimer;
        } else {
            timer = bothPresentMismatchTimer;
        }

        timer.record(durationNanos, TimeUnit.NANOSECONDS);
    }
}
