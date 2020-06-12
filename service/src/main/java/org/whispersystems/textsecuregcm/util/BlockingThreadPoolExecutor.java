package org.whispersystems.textsecuregcm.util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class BlockingThreadPoolExecutor extends ThreadPoolExecutor {

  private final Semaphore semaphore;
  private final Timer     acquirePermitTimer;

  public BlockingThreadPoolExecutor(String name, int threads, int bound) {
    super(threads, threads, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    this.semaphore = new Semaphore(bound);

    final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

    this.acquirePermitTimer = metricRegistry.timer(name(getClass(), name, "acquirePermit"));
    metricRegistry.gauge(name(getClass(), name, "permitsAvailable"), () -> semaphore::availablePermits);
  }

  @Override
  public void execute(Runnable task) {
    try (final Timer.Context ignored = acquirePermitTimer.time()) {
      semaphore.acquireUninterruptibly();
    }

    try {
      super.execute(task);
    } catch (Throwable t) {
      semaphore.release();
      throw new RuntimeException(t);
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    semaphore.release();
  }

  public int getSize() {
    return ((LinkedBlockingQueue)getQueue()).size();
  }
}