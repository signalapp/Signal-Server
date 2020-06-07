package org.whispersystems.textsecuregcm.experiment;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * An experiment compares the results of two operations and records metrics to assess how frequently they match.
 */
public class Experiment {

    private final String        counterName;
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

        this.counterName   = MetricRegistry.name(getClass(), names);
        this.meterRegistry = meterRegistry;
    }

    public <T> void compareResult(final T expected, final CompletionStage<T> experimentStage) {
        experimentStage.whenComplete((actual, cause) -> {
            if (cause != null) {
                meterRegistry.counter(counterName,
                        List.of(Tag.of(OUTCOME_TAG, ERROR_OUTCOME), Tag.of(CAUSE_TAG, cause.getClass().getSimpleName())))
                        .increment();
            } else {
                final boolean shouldIgnore = actual == null && expected != null;

                if (!shouldIgnore) {
                    meterRegistry.counter(counterName,
                            List.of(Tag.of(OUTCOME_TAG, Objects.equals(expected, actual) ? MATCH_OUTCOME : MISMATCH_OUTCOME)))
                            .increment();
                }
            }
        });
    }
}
