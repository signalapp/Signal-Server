package org.whispersystems.textsecuregcm.tests.util;

import com.google.common.util.concurrent.SettableFuture;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SynchronousExecutorService implements ExecutorService {

  private boolean shutdown = false;

  @Override
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return shutdown;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return true;
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    SettableFuture<T> future = null;
    try {
      future = SettableFuture.create();
      future.set(task.call());
    } catch (Throwable e) {
      future.setException(e);
    }

    return future;
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    SettableFuture<T> future = SettableFuture.create();
    task.run();

    future.set(result);

    return future;
  }

  @Override
  public Future<?> submit(Runnable task) {
    SettableFuture future = SettableFuture.create();
    task.run();
    future.set(null);
    return future;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    List<Future<T>> results = new LinkedList<>();
    for (Callable<T> callable : tasks) {
      SettableFuture<T> future = SettableFuture.create();
      try {
        future.set(callable.call());
      } catch (Throwable e) {
        future.setException(e);
      }
      results.add(future);
    }
    return results;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
    return invokeAll(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  @Override
  public void execute(Runnable command) {
    command.run();
  }
}
