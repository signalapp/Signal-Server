/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

public abstract class ManagedPeriodicWork implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ManagedPeriodicWork.class);

  private final ManagedPeriodicWorkCache cache;
  private final Duration workerTtl;
  private final Duration runInterval;
  private final String workerId;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private boolean finished;

  public ManagedPeriodicWork(final ManagedPeriodicWorkCache cache, final Duration workerTtl, final Duration runInterval) {
    this.cache = cache;
    this.workerTtl = workerTtl;
    this.runInterval = runInterval;
    this.workerId = UUID.randomUUID().toString();
  }

  abstract protected void doPeriodicWork() throws Exception;

  @Override
  public synchronized void start() throws Exception {
    running.set(true);
    new Thread(this).start();
  }

  @Override
  public synchronized void stop() throws Exception {

    running.set(false);
    notifyAll();

    while (!finished) {
      Util.wait(this);
    }
  }

  @Override
  public void run() {

    while(running.get()) {
      try {
        execute();
        sleepWhileRunning(runInterval);
      } catch (final Exception e) {
        logger.warn("Error in crawl crawl", e);

        // wait a bit, in case the error is caused by external instability
        Util.sleep(10_000);
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  private void execute() {

    if (cache.claimActiveWork(workerId, workerTtl)) {

      try {
        final long startTimeMs = System.currentTimeMillis();

        doPeriodicWork();

        final long endTimeMs = System.currentTimeMillis();
        final Duration sleepInterval = runInterval.minusMillis(endTimeMs - startTimeMs);
        if (sleepInterval.getSeconds() > 0) {
          sleepWhileRunning(sleepInterval);
        }

      } catch (final Exception e) {
        logger.warn("Failed to process chunk", e);

        // wait a full interval for recovery
        sleepWhileRunning(runInterval);

      } finally {
        cache.releaseActiveWork(workerId);
      }
    }
  }

  private synchronized void sleepWhileRunning(Duration delay) {
    if (running.get()) Util.wait(this, delay.toMillis());
  }
}
