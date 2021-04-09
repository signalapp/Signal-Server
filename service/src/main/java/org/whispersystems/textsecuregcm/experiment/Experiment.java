/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * An experiment compares the results of two operations and records metrics to assess how frequently they match.
 */
public class Experiment {

    private final String name;

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

    private static final Logger log = LoggerFactory.getLogger(Experiment.class);

    public Experiment(final String... names) {
        this(name(Experiment.class, names),
             Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MATCH_OUTCOME),
             Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, ERROR_OUTCOME),
             Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG, BOTH_PRESENT_MISMATCH),
             Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG, CONTROL_NULL_MISMATCH),
             Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG, EXPERIMENT_NULL_MISMATCH));
    }

    @VisibleForTesting
    Experiment(final String name, final Timer matchTimer, final Timer errorTimer, final Timer bothPresentMismatchTimer, final Timer controlNullMismatchTimer, final Timer experimentNullMismatchTimer) {
        this.name = name;

        this.matchTimer = matchTimer;
        this.errorTimer = errorTimer;

        this.bothPresentMismatchTimer = bothPresentMismatchTimer;
        this.controlNullMismatchTimer = controlNullMismatchTimer;
        this.experimentNullMismatchTimer = experimentNullMismatchTimer;
    }

    public <T> void compareFutureResult(final T expected, final CompletionStage<T> experimentStage) {
        final long startNanos = System.nanoTime();

        experimentStage.whenComplete((actual, cause) -> {
            final long durationNanos = System.nanoTime() - startNanos;

            if (cause != null) {
                recordError(cause, durationNanos);
            } else {
                recordResult(expected, actual, durationNanos);
            }
        });
    }

    public <T> void compareSupplierResult(final T expected, final Supplier<T> experimentSupplier) {
        final long startNanos = System.nanoTime();

        try {
            final T result = experimentSupplier.get();

            recordResult(expected, result, System.nanoTime() - startNanos);
        } catch (final Exception e) {
            recordError(e, System.nanoTime() - startNanos);
        }
    }

    public <T> void compareSupplierResultAsync(final T expected, final Supplier<T> experimentSupplier, final Executor executor) {
        final long startNanos = System.nanoTime();

        try {
            compareFutureResult(expected, CompletableFuture.supplyAsync(experimentSupplier, executor));
        } catch (final Exception e) {
            recordError(e, System.nanoTime() - startNanos);
        }
    }

    private void recordError(final Throwable cause, final long durationNanos) {
        log.warn("Experiment {} threw an exception.", name, cause);
        errorTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    @VisibleForTesting
    <T> void recordResult(final T expected, final T actual, final long durationNanos) {
        if (expected instanceof Optional && actual instanceof Optional) {
            recordResult(((Optional)expected).orElse(null), ((Optional)actual).orElse(null), durationNanos);
        } else {
            final Timer Timer;

            if (Objects.equals(expected, actual)) {
                Timer = matchTimer;
            } else if (expected == null) {
                Timer = controlNullMismatchTimer;
            } else if (actual == null) {
                Timer = experimentNullMismatchTimer;
            } else {
                Timer = bothPresentMismatchTimer;
            }

            Timer.record(durationNanos, TimeUnit.NANOSECONDS);
        }
    }
}
