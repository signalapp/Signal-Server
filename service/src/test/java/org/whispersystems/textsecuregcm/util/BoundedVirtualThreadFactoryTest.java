/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BoundedVirtualThreadFactoryTest {

  private final static int MAX_THREADS = 10;

  private BoundedVirtualThreadFactory factory;

  @BeforeEach
  void setUp() {
    factory = new BoundedVirtualThreadFactory("test", MAX_THREADS);
  }

  @Test
  void releaseWhenTaskThrows() throws InterruptedException {
    final UncaughtExceptionHolder uncaughtExceptionHolder = new UncaughtExceptionHolder();
    final Thread t = submit(() -> {
      throw new IllegalArgumentException("test");
    }, uncaughtExceptionHolder);
    assertThat(t).isNotNull();
    t.join(Duration.ofSeconds(1));
    assertThat(uncaughtExceptionHolder.exception).isNotNull().isInstanceOf(IllegalArgumentException.class);

    submitUntilRejected();
  }

  @Test
  void releaseWhenRejected() throws InterruptedException {
    submitUntilRejected();
    submitUntilRejected();
  }

  @Test
  void executorServiceRejectsAtLimit() throws InterruptedException {
    try (final ExecutorService executor = Executors.newThreadPerTaskExecutor(factory)) {

      final CountDownLatch latch = new CountDownLatch(1);
      for (int i = 0; i < MAX_THREADS; i++) {
        executor.submit(() -> {
          try {
            latch.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
      }
      assertThatExceptionOfType(RejectedExecutionException.class).isThrownBy(() -> executor.submit(Util.NOOP));
      latch.countDown();

      executor.shutdown();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void stressTest() throws InterruptedException {
    for (int iteration = 0; iteration < 50; iteration++) {
      final CountDownLatch latch = new CountDownLatch(MAX_THREADS);

      // submit a task that submits a task maxThreads/2 times
      final Thread[] threads = new Thread[MAX_THREADS];
      for (int i = 0; i < MAX_THREADS; i+=2) {
        int outerThreadIndex = i;
        int innerThreadIndex = i + 1;

        threads[outerThreadIndex] = submit(() -> {
          latch.countDown();
          threads[innerThreadIndex] = submit(latch::countDown);
        });
      }
      latch.await();

      // All threads must be created at this point, wait for them all to complete
      for (Thread thread : threads) {
        assertThat(thread).isNotNull();
        thread.join();
      }

      assertThat(factory.getRunningThreads()).isEqualTo(0);
    }

    submitUntilRejected();
  }

  /**
   * Verify we can submit up to the concurrency limit (and no more)
   */
  private void submitUntilRejected() throws InterruptedException {
    final CountDownLatch finish = new CountDownLatch(1);
    final List<Thread> threads = IntStream.range(0, MAX_THREADS).mapToObj(_ -> submit(() -> {
      try {
        finish.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    })).toList();

    assertThat(submit(Util.NOOP)).isNull();

    finish.countDown();

    for (Thread thread : threads) {
      thread.join();
    }
    assertThat(factory.getRunningThreads()).isEqualTo(0);
  }

  private Thread submit(final Runnable runnable) {
    return submit(runnable, (t, e) ->
      fail("Uncaught exception on thread {} : {}", t, e));
  }

  private Thread submit(final Runnable runnable, final Thread.UncaughtExceptionHandler handler) {
    final Thread thread = factory.newThread(runnable);
    if (thread == null) {
      return null;
    }
    if (handler != null) {
      thread.setUncaughtExceptionHandler(handler);
    }
    thread.start();
    return thread;
  }

  private static class UncaughtExceptionHolder implements Thread.UncaughtExceptionHandler {
    Throwable exception = null;

    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
        exception = e;
    }
  }

}
