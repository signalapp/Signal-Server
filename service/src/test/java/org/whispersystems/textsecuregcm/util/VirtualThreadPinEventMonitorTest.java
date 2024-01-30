/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import jdk.jfr.consumer.RecordedEvent;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public class VirtualThreadPinEventMonitorTest {
  private void synchronizedSleep1() {
    synchronized (this) {
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void synchronizedSleep2() {
    synchronized (this) {
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testPinEventProduced() throws InterruptedException, ExecutionException {
    final BlockingQueue<Pair<RecordedEvent, Boolean>> bq = new LinkedBlockingQueue<>();
    final ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
    VirtualThreadPinEventMonitor eventMonitor = queueingLogger(exec, Set.of(), bq);
    eventMonitor.start();
    // give start a moment to begin the event stream thread
    Thread.sleep(100);
    exec.submit(() -> synchronizedSleep1()).get();
    eventMonitor.stop();

    final Pair<RecordedEvent, Boolean> nxt = bq.take();
    assertThat(nxt.getRight()).isFalse();
    assertThat(bq.isEmpty());
    exec.shutdown();
    exec.awaitTermination(1, TimeUnit.MILLISECONDS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"VirtualThreadPinEventMonitorTest.synchronizedSleep1", "synchronizedSleep1"})
  public void testPinEventFiltered(final String allowString) throws InterruptedException, ExecutionException {
    final BlockingQueue<Pair<RecordedEvent, Boolean>> bq = new LinkedBlockingQueue<>();
    final ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
    final VirtualThreadPinEventMonitor eventMonitor = queueingLogger(exec, Set.of(allowString), bq);
    eventMonitor.start();
    // give start a moment to begin the event stream thread
    Thread.sleep(100);
    exec.submit(() -> synchronizedSleep1()).get();
    exec.submit(() -> synchronizedSleep2()).get();
    eventMonitor.stop();

    final boolean sleep1Allowed = bq.take().getRight();
    final boolean sleep2Allowed = bq.take().getRight();
    assertThat(sleep1Allowed).isTrue();
    assertThat(sleep2Allowed).isFalse();
    assertThat(bq.isEmpty());
    exec.shutdown();
    exec.awaitTermination(1, TimeUnit.MILLISECONDS);
  }

  private static VirtualThreadPinEventMonitor queueingLogger(
      final ExecutorService exec,
      final Set<String> allowedMethods,
      final BlockingQueue<Pair<RecordedEvent, Boolean>> bq) {
    return new VirtualThreadPinEventMonitor(exec,
        () -> allowedMethods,
        Duration.ofMillis(1),
        (event, allowed) -> {
          try {
            bq.put(Pair.of(event, allowed));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

  }
}
