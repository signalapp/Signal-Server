/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

public abstract class ManagedPeriodicWork implements Managed {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final ManagedPeriodicWorkLock lock;
  private final Duration workerTtl;
  private final Duration runInterval;
  private final String workerId;
  private final ScheduledExecutorService executorService;

  private final AtomicBoolean started = new AtomicBoolean(false);


  public ManagedPeriodicWork(final ManagedPeriodicWorkLock lock, final Duration workerTtl, final Duration runInterval) {
    this.lock = lock;
    this.workerTtl = workerTtl;
    this.runInterval = runInterval;
    this.workerId = UUID.randomUUID().toString();
    this.executorService = Executors.newSingleThreadScheduledExecutor((runnable) -> new Thread(runnable, getClass().getName()));
  }

  abstract protected void doPeriodicWork() throws Exception;

  @Override
  public synchronized void start() throws Exception {

    if (started.getAndSet(true)) {
      return;
    }

    executorService.scheduleAtFixedRate(() -> {
        try {
          execute();
        } catch (final Exception e) {
          logger.warn("Error in execution", e);

          // wait a bit, in case the error is caused by external instability
          Util.sleep(10_000);
        }
    }, 0, runInterval.getSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public synchronized void stop() throws Exception {

    executorService.shutdown();

    boolean terminated = false;
    while (!terminated) {
      terminated = executorService.awaitTermination(5, TimeUnit.MINUTES);
      if (!terminated) {
        logger.warn("worker not yet terminated");
      }
    }
  }

  private void execute() {

    if (lock.claimActiveWork(workerId, workerTtl)) {
      try {
        logger.info("Starting execution");
        doPeriodicWork();
        logger.info("Execution complete");

      } catch (final Exception e) {
        logger.warn("Periodic work failed", e);

        // wait a bit, in case the error is caused by external instability
        Util.sleep(10_000);

      } finally {
        lock.releaseActiveWork(workerId);
      }
    }
  }
}
