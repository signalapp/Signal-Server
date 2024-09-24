/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;
import reactor.core.publisher.Mono;
import reactor.test.publisher.TestPublisher;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ProcessScheduledJobsServiceCommandTest {

  private ScheduledExecutorService scheduledExecutorService;

  @BeforeEach
  void setUp() {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Test
  void testDisposeOnStopCancels() throws Exception {
    // This test publisher will never emit any values or intentionally complete
    final TestPublisher<Integer> testPublisher = TestPublisher.create();
    final TestJobScheduler testJobScheduler = new TestJobScheduler(testPublisher);

    final ProcessScheduledJobsServiceCommand.ScheduledJobProcessor scheduledJobProcessor =
        new ProcessScheduledJobsServiceCommand.ScheduledJobProcessor(testJobScheduler, scheduledExecutorService, 60);

    scheduledJobProcessor.start();
    testJobScheduler.getStartLatch().await();

    scheduledJobProcessor.stop();
    testJobScheduler.getEndLatch().await();

    scheduledExecutorService.shutdown();
    assertTrue(scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS), "The submitted task should complete");

    testPublisher.assertCancelled();
  }

  @Test
  void testCompletedPublisher() throws Exception {
    final TestPublisher<Integer> testPublisher = TestPublisher.create();
    testPublisher.complete();

    final TestJobScheduler testJobScheduler = new TestJobScheduler(testPublisher);

    final ProcessScheduledJobsServiceCommand.ScheduledJobProcessor scheduledJobProcessor =
        new ProcessScheduledJobsServiceCommand.ScheduledJobProcessor(testJobScheduler, scheduledExecutorService, 60);

    scheduledJobProcessor.start();
    testJobScheduler.getStartLatch().await();

    scheduledJobProcessor.stop();
    testJobScheduler.getEndLatch().await();

    scheduledExecutorService.shutdown();
    assertTrue(scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS), "The submitted task should complete");

    testPublisher.assertNotCancelled();
  }

  private static class TestJobScheduler extends JobScheduler {

    private final TestPublisher<Integer> testPublisher;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch endLatch = new CountDownLatch(1);

    protected TestJobScheduler(TestPublisher<Integer> testPublisher) {
      super(null, null, null, null);
      this.testPublisher = testPublisher;
    }

    /**
     * A {@link CountDownLatch} indicating whether the {@link Mono} returned by {@link #processAvailableJobs()} has been
     * subscribed to.
     */
    public CountDownLatch getStartLatch() {
      return startLatch;
    }

    /**
     * A {@link CountDownLatch} indicating whether the {@link Mono} returned by {@link #processAvailableJobs()} has
     * terminated or been canceled.
     */
    public CountDownLatch getEndLatch() {
      return endLatch;
    }

    @Override
    public String getSchedulerName() {
      return "test";
    }

    @Override
    protected CompletableFuture<String> processJob(@Nullable byte[] jobData) {
      return CompletableFuture.failedFuture(new IllegalStateException("Not implemented"));
    }

    @Override
    public Mono<Void> processAvailableJobs() {
      return testPublisher.flux()
          .then()
          .doOnSubscribe(ignored -> startLatch.countDown())
          .doOnTerminate(endLatch::countDown)
          .doOnCancel(endLatch::countDown);
    }
  }

}
