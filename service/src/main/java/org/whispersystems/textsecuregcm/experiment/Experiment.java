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

    private final Timer  matchTimer;
    private final Timer  mismatchTimer;
    private final Timer  errorTimer;

    static final String OUTCOME_TAG      = "outcome";
    static final String MATCH_OUTCOME    = "match";
    static final String MISMATCH_OUTCOME = "mismatch";
    static final String ERROR_OUTCOME    = "error";

    public Experiment(final String... names) {
        this(buildTimer(MATCH_OUTCOME, names), buildTimer(MISMATCH_OUTCOME, names), buildTimer(ERROR_OUTCOME, names));
    }

    private static Timer buildTimer(final String outcome, final String... names) {
        if (names == null || names.length == 0) {
            throw new IllegalArgumentException("Experiments must have a name");
        }

        return Timer.builder(MetricRegistry.name(Experiment.class, names))
                .tag(OUTCOME_TAG, outcome)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry);
    }

    @VisibleForTesting
    Experiment(final Timer matchTimer, final Timer mismatchTimer, final Timer errorTimer) {
        this.matchTimer = matchTimer;
        this.mismatchTimer = mismatchTimer;
        this.errorTimer = errorTimer;
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
        final boolean shouldIgnore = actual == null && expected != null;

        if (!shouldIgnore) {
            final Timer timer = Objects.equals(expected, actual) ? matchTimer : mismatchTimer;
            timer.record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }
}
