/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.glassfish.jersey.server.ManagedAsyncExecutor;
import org.glassfish.jersey.spi.ExecutorServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManagedAsyncExecutor
public class VirtualExecutorServiceProvider implements ExecutorServiceProvider {
  private static final Logger logger = LoggerFactory.getLogger(VirtualExecutorServiceProvider.class);


  /**
   * Default thread pool executor termination timeout in milliseconds.
   */
  public static final int TERMINATION_TIMEOUT = 5000;
  private final String virtualThreadNamePrefix;

  public VirtualExecutorServiceProvider(final String virtualThreadNamePrefix) {
    this.virtualThreadNamePrefix = virtualThreadNamePrefix;
  }


  @Override
  public ExecutorService getExecutorService() {
    logger.info("Creating executor service with virtual thread per task");
    return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(virtualThreadNamePrefix, 0).factory());
  }

  @Override
  public void dispose(final ExecutorService executorService) {
    logger.info("Shutting down virtual thread pool executor");

    executorService.shutdown();
    boolean terminated = false;
    try {
      terminated = executorService.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (!terminated) {
      // virtual thread per task executor has no queue, so shouldn't have any un-run tasks
      final List<Runnable> unrunTasks = executorService.shutdownNow();
      logger.info("Force terminated executor with {} un-run tasks", unrunTasks.size());
    }
  }
}
