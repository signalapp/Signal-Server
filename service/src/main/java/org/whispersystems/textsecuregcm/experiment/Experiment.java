/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

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

  private static final String OUTCOME_TAG = "outcome";
  private static final String MATCH_OUTCOME = "match";
  private static final String MISMATCH_OUTCOME = "mismatch";
  private static final String ERROR_OUTCOME = "error";

  private static final String MISMATCH_TYPE_TAG = "mismatchType";
  private static final String BOTH_PRESENT_MISMATCH = "bothPresent";
  private static final String CONTROL_NULL_MISMATCH = "controlResultNull";
  private static final String EXPERIMENT_NULL_MISMATCH = "experimentResultNull";

  private static final Logger log = LoggerFactory.getLogger(Experiment.class);

  public Experiment(final String... names) {
    this(name(Experiment.class, names),
        Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MATCH_OUTCOME),
        Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, ERROR_OUTCOME),
        Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG,
            BOTH_PRESENT_MISMATCH),
        Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG,
            CONTROL_NULL_MISMATCH),
        Metrics.timer(name(Experiment.class, names), OUTCOME_TAG, MISMATCH_OUTCOME, MISMATCH_TYPE_TAG,
            EXPERIMENT_NULL_MISMATCH));
  }

  @VisibleForTesting
  Experiment(final String name, final Timer matchTimer, final Timer errorTimer, final Timer bothPresentMismatchTimer,
      final Timer controlNullMismatchTimer, final Timer experimentNullMismatchTimer) {
    this.name = name;

    this.matchTimer = matchTimer;
    this.errorTimer = errorTimer;

    this.bothPresentMismatchTimer = bothPresentMismatchTimer;
    this.controlNullMismatchTimer = controlNullMismatchTimer;
    this.experimentNullMismatchTimer = experimentNullMismatchTimer;
  }

  public <T> void compareMonoResult(final T expected, final Mono<T> experimentMono) {
    final Timer.Sample sample = Timer.start();

    experimentMono.subscribe(
        actual -> recordResult(expected, actual, sample),
        cause -> recordError(cause, sample));
  }

  public <T> void compareFutureResult(final T expected, final CompletionStage<T> experimentStage) {
    final Timer.Sample sample = Timer.start();

    experimentStage.whenComplete((actual, cause) -> {
      if (cause != null) {
        recordError(cause, sample);
      } else {
        recordResult(expected, actual, sample);
      }
    });
  }

  public <T> void compareSupplierResult(final T expected, final Supplier<T> experimentSupplier) {
    final Timer.Sample sample = Timer.start();

    try {
      final T result = experimentSupplier.get();

      recordResult(expected, result, sample);
    } catch (final Exception e) {
      recordError(e, sample);
    }
  }

  public <T> void compareSupplierResultAsync(final T expected, final Supplier<T> experimentSupplier, final Executor executor) {
    final Timer.Sample sample = Timer.start();

    try {
      compareFutureResult(expected, CompletableFuture.supplyAsync(experimentSupplier, executor));
    } catch (final Exception e) {
      recordError(e, sample);
    }
  }

  private void recordError(final Throwable cause, final Timer.Sample sample) {
    log.warn("Experiment {} threw an exception.", name, cause);
    sample.stop(errorTimer);
  }

  @VisibleForTesting
  <T> void recordResult(final T expected, final T actual, final Timer.Sample sample) {
    if (expected instanceof Optional && actual instanceof Optional) {
      recordResult(((Optional) expected).orElse(null), ((Optional) actual).orElse(null), sample);
    } else {
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

      sample.stop(timer);
    }
  }
}
