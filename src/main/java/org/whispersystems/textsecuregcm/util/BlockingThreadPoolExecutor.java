package org.whispersystems.textsecuregcm.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BlockingThreadPoolExecutor extends ThreadPoolExecutor {

  private final Semaphore semaphore;

  public BlockingThreadPoolExecutor(int threads, int bound) {
    super(threads, threads, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    this.semaphore = new Semaphore(bound);
  }

  @Override
  public void execute(Runnable task) {
    semaphore.acquireUninterruptibly();

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