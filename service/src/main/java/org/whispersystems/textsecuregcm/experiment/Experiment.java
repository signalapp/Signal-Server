package org.whispersystems.textsecuregcm.experiment;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * An experiment compares the results of two operations and records metrics to assess how frequently they match.
 */
public class Experiment {

    private final String timerName;
    private final MeterRegistry meterRegistry;

    static final String OUTCOME_TAG = "outcome";
    static final String CAUSE_TAG   = "cause";

    static final String MATCH_OUTCOME    = "match";
    static final String MISMATCH_OUTCOME = "mismatch";
    static final String ERROR_OUTCOME    = "error";

    public Experiment(final String... names) {
        this(Metrics.globalRegistry, names);
    }

    @VisibleForTesting
    Experiment(final MeterRegistry meterRegistry, final String... names) {
        if (names == null || names.length == 0) {
            throw new IllegalArgumentException("Experiments must have a name");
        }

        this.timerName     = MetricRegistry.name(getClass(), names);
        this.meterRegistry = meterRegistry;
    }

    public <T> void compareResult(final T expected, final CompletionStage<T> experimentStage) {
        // We're assuming that we get the experiment completion stage as soon as possible after it's started, but this
        // start time will inescapably have some "wiggle" to it.
        final long start = System.nanoTime();

        experimentStage.whenComplete((actual, cause) -> {
            final long duration = System.nanoTime() - start;

            if (cause != null) {
                meterRegistry.timer(timerName,
                        List.of(Tag.of(OUTCOME_TAG, ERROR_OUTCOME), Tag.of(CAUSE_TAG, cause.getClass().getSimpleName())))
                        .record(duration, TimeUnit.NANOSECONDS);
            } else {
                final boolean shouldIgnore = actual == null && expected != null;

                if (!shouldIgnore) {
                    meterRegistry.timer(timerName,
                            List.of(Tag.of(OUTCOME_TAG, Objects.equals(expected, actual) ? MATCH_OUTCOME : MISMATCH_OUTCOME)))
                            .record(duration, TimeUnit.NANOSECONDS);
                }
            }
        });
    }
}
