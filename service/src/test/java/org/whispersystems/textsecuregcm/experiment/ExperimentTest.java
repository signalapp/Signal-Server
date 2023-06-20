/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExperimentTest {

  private Timer matchTimer;
  private Timer errorTimer;

  private Experiment experiment;

  @BeforeEach
  void setUp() {
    matchTimer = mock(Timer.class);
    errorTimer = mock(Timer.class);

    experiment = new Experiment("test", matchTimer, errorTimer, mock(Timer.class), mock(Timer.class),
        mock(Timer.class));
  }

  @Test
  void compareFutureResult() {
    experiment.compareFutureResult(12, CompletableFuture.completedFuture(12));
    verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  void compareFutureResultError() {
    experiment.compareFutureResult(12, CompletableFuture.failedFuture(new RuntimeException("OH NO")));
    verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  void compareSupplierResultMatch() {
    experiment.compareSupplierResult(12, () -> 12);
    verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  void compareSupplierResultError() {
    experiment.compareSupplierResult(12, () -> {
      throw new RuntimeException("OH NO");
    });
    verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  void compareSupplierResultAsyncMatch() throws InterruptedException {
    final ExecutorService experimentExecutor = Executors.newSingleThreadExecutor();

    experiment.compareSupplierResultAsync(12, () -> 12, experimentExecutor);
    experimentExecutor.shutdown();
    experimentExecutor.awaitTermination(1, TimeUnit.SECONDS);

    verify(matchTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  void compareSupplierResultAsyncError() throws InterruptedException {
    final ExecutorService experimentExecutor = Executors.newSingleThreadExecutor();

    experiment.compareSupplierResultAsync(12, () -> {
      throw new RuntimeException("OH NO");
    }, experimentExecutor);
    experimentExecutor.shutdown();
    experimentExecutor.awaitTermination(1, TimeUnit.SECONDS);

    verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  void compareSupplierResultAsyncRejection() {
    final ExecutorService executorService = mock(ExecutorService.class);
    doThrow(new RejectedExecutionException()).when(executorService).execute(any(Runnable.class));

    experiment.compareSupplierResultAsync(12, () -> 12, executorService);
    verify(errorTimer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @ParameterizedTest
  @MethodSource
  public void testRecordResult(final Object expected, final Object actual, final Experiment experiment, final Timer expectedTimer) {
    reset(expectedTimer);

    final MockClock clock = new MockClock();
    final Timer.Sample sample = Timer.start(clock);

    final long durationNanos = 123;
    clock.add(durationNanos, TimeUnit.NANOSECONDS);

    experiment.recordResult(expected, actual, sample);
    verify(expectedTimer).record(durationNanos, TimeUnit.NANOSECONDS);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> testRecordResult() {
    // Hack: parameters are set before the @Before method gets called
    final Timer matchTimer = mock(Timer.class);
    final Timer errorTimer = mock(Timer.class);
    final Timer bothPresentMismatchTimer = mock(Timer.class);
    final Timer controlNullMismatchTimer = mock(Timer.class);
    final Timer experimentNullMismatchTimer = mock(Timer.class);

    final Experiment experiment = new Experiment("test", matchTimer, errorTimer, bothPresentMismatchTimer,
        controlNullMismatchTimer, experimentNullMismatchTimer);

    return Stream.of(
        Arguments.of(12, 12, experiment, matchTimer),
        Arguments.of(null, 12, experiment, controlNullMismatchTimer),
        Arguments.of(12, null, experiment, experimentNullMismatchTimer),
        Arguments.of(12, 17, experiment, bothPresentMismatchTimer),
        Arguments.of(Optional.of(12), Optional.of(12), experiment, matchTimer),
        Arguments.of(Optional.empty(), Optional.of(12), experiment, controlNullMismatchTimer),
        Arguments.of(Optional.of(12), Optional.empty(), experiment, experimentNullMismatchTimer),
        Arguments.of(Optional.of(12), Optional.of(17), experiment, bothPresentMismatchTimer)
    );
  }
}
