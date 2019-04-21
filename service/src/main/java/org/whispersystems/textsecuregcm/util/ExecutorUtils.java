package org.whispersystems.textsecuregcm.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorUtils {

  public static Executor newFixedThreadBoundedQueueExecutor(int threadCount, int queueSize) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount,
                                                         Long.MAX_VALUE, TimeUnit.NANOSECONDS,
                                                         new ArrayBlockingQueue<>(queueSize),
                                                         new ThreadPoolExecutor.AbortPolicy());

    executor.prestartAllCoreThreads();

    return executor;
  }

}
