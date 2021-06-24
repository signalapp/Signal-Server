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

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final ManagedPeriodicWorkLock lock;
  private final Duration workerTtl;
  private final Duration runInterval;
  private final String workerId;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private boolean finished;

  public ManagedPeriodicWork(final ManagedPeriodicWorkLock lock, final Duration workerTtl, final Duration runInterval) {
    this.lock = lock;
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
        logger.warn("Error in execution", e);

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

    if (lock.claimActiveWork(workerId, workerTtl)) {

      try {
        final long startTimeMs = System.currentTimeMillis();

        logger.info("Starting execution");
        doPeriodicWork();
        logger.info("Execution complete");

        final long endTimeMs = System.currentTimeMillis();
        final Duration sleepInterval = runInterval.minusMillis(endTimeMs - startTimeMs);
        if (sleepInterval.getSeconds() > 0) {
          logger.info("Sleeping for {}", sleepInterval);
          sleepWhileRunning(sleepInterval);
        }

      } catch (final Exception e) {
        logger.warn("Periodic work failed", e);

        // wait a full interval for recovery
        sleepWhileRunning(runInterval);

      } finally {
        lock.releaseActiveWork(workerId);
      }
    }
  }

  private synchronized void sleepWhileRunning(Duration delay) {
    if (running.get()) Util.wait(this, delay.toMillis());
  }
}
