/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/**
 * A thread factory that creates virtual threads but limits the total number of virtual threads created.
 */
public class BoundedVirtualThreadFactory implements ThreadFactory {

  private static final Logger logger = LoggerFactory.getLogger(BoundedVirtualThreadFactory.class);

  private final AtomicInteger runningThreads = new AtomicInteger();
  private final ThreadFactory delegate;
  private final int maxConcurrentThreads;

  private final Counter created;
  private final Counter completed;

  public BoundedVirtualThreadFactory(final String threadPoolName, final int maxConcurrentThreads) {
    this.maxConcurrentThreads = maxConcurrentThreads;

    final Tags tags = Tags.of("pool", threadPoolName);
    Metrics.gauge(
        MetricsUtil.name(BoundedVirtualThreadFactory.class, "active"),
        tags, runningThreads, (rt) -> (double) rt.get());
    this.created = Metrics.counter(MetricsUtil.name(BoundedVirtualThreadFactory.class, "created"), tags);
    this.completed = Metrics.counter(MetricsUtil.name(BoundedVirtualThreadFactory.class, "completed"), tags);

    // The virtual thread factory will initialize thread names by appending the thread index to the provided prefix
    this.delegate = Thread.ofVirtual().name(threadPoolName + "-", 0).factory();

  }

  @Override
  public Thread newThread(final Runnable r) {
    if (!tryAcquire()) {
      return null;
    }
    Thread thread = null;
    try {
      final Runnable wrapped = () -> {
        try {
          r.run();
        } finally {
          release();
        }
      };
      thread = delegate.newThread(wrapped);
    } finally {
      if (thread == null) {
        release();
      }
    }
    return thread;
  }


  @VisibleForTesting
  int getRunningThreads() {
    return runningThreads.get();
  }

  private boolean tryAcquire() {
    int old;
    do {
      old = runningThreads.get();
      if (old >= maxConcurrentThreads) {
        return false;
      }
    } while (!runningThreads.compareAndSet(old, old + 1));
    created.increment();
    return true;
  }

  private void release() {
    int updated = runningThreads.decrementAndGet();
    if (updated < 0) {
      logger.error("Released a thread and count was {}, which should never happen", updated);
    }
    completed.increment();
  }
}
