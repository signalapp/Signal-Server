/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.Util;

class ManagedPeriodicWorkTest {

  private ScheduledExecutorService scheduledExecutorService;
  private ManagedPeriodicWorkLock lock;
  private TestWork testWork;

  @BeforeEach
  void setup() {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    lock = mock(ManagedPeriodicWorkLock.class);

    testWork = new TestWork(lock, Duration.ofMinutes(5), Duration.ofMinutes(5),
        scheduledExecutorService);
  }

  @AfterEach
  void teardown() throws Exception {
    scheduledExecutorService.shutdown();

    assertTrue(scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test
  void test() throws Exception {
    when(lock.claimActiveWork(any(), any())).thenReturn(true);

    testWork.start();

    synchronized (testWork) {
      Util.wait(testWork);
    }

    testWork.stop();

    verify(lock, times(1)).claimActiveWork(anyString(), any(Duration.class));
    verify(lock, times(1)).releaseActiveWork(anyString());

    assertEquals(1, testWork.getCount());
  }

  @Test
  void testSlowWorkShutdown() throws Exception {

    when(lock.claimActiveWork(any(), any())).thenReturn(true);

    testWork.setWorkSleepDuration(Duration.ofSeconds(1));

    testWork.start();

    synchronized (testWork) {
      Util.wait(testWork);
    }

    long startMillis = System.currentTimeMillis();

    testWork.stop();

    long runMillis = System.currentTimeMillis() - startMillis;

    assertTrue(runMillis > 500);

    verify(lock, times(1)).claimActiveWork(anyString(), any(Duration.class));
    verify(lock, times(1)).releaseActiveWork(anyString());

    assertEquals(1, testWork.getCount());
  }

  @Test
  void testWorkExceptionReleasesLock() throws Exception {
    when(lock.claimActiveWork(any(), any())).thenReturn(true);

    testWork = new ExceptionalTestWork(lock, Duration.ofMinutes(5), Duration.ofMinutes(5), scheduledExecutorService);

    testWork.setSleepDurationAfterUnexpectedException(Duration.ZERO);

    testWork.start();

    synchronized (testWork) {
      Util.wait(testWork);
    }

    testWork.stop();

    verify(lock, times(1)).claimActiveWork(anyString(), any(Duration.class));
    verify(lock, times(1)).releaseActiveWork(anyString());

    assertEquals(0, testWork.getCount());
  }


  private static class TestWork extends ManagedPeriodicWork {

    private final AtomicInteger workCounter = new AtomicInteger();
    private Duration workSleepDuration = Duration.ZERO;

    public TestWork(final ManagedPeriodicWorkLock lock, final Duration workerTtl, final Duration runInterval,
        final ScheduledExecutorService scheduledExecutorService) {
      super(lock, workerTtl, runInterval, scheduledExecutorService);
    }

    @Override
    protected void doPeriodicWork() throws Exception {

      notifyStarted();

      if (!workSleepDuration.isZero()) {
        Util.sleep(workSleepDuration.toMillis());
      }

      workCounter.incrementAndGet();
    }

    synchronized void notifyStarted() {
      notifyAll();
    }

    int getCount() {
      return workCounter.get();
    }

    void setWorkSleepDuration(final Duration workSleepDuration) {
      this.workSleepDuration = workSleepDuration;
    }
  }

  private static class ExceptionalTestWork extends TestWork {


    public ExceptionalTestWork(final ManagedPeriodicWorkLock lock, final Duration workerTtl, final Duration runInterval,
        final ScheduledExecutorService scheduledExecutorService) {
      super(lock, workerTtl, runInterval, scheduledExecutorService);
    }

    @Override
    protected void doPeriodicWork() throws Exception {

      notifyStarted();

      throw new RuntimeException();
    }
  }

}
