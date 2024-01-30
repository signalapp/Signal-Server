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
import org.junit.jupiter.api.Disabled;
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
  @Disabled("flaky: no way to ensure the sequencing between the start of the recording stream and emitting the event")
  public void testPinEventProduced() throws InterruptedException, ExecutionException {
    final BlockingQueue<Pair<RecordedEvent, Boolean>> bq = new LinkedBlockingQueue<>();
    final ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
    VirtualThreadPinEventMonitor eventMonitor = queueingLogger(exec, Set.of(), bq);
    eventMonitor.start();
    // give start a moment to begin the event stream thread
    Thread.sleep(100);
    exec.submit(() -> synchronizedSleep1()).get();
    eventMonitor.stop();

    final Pair<RecordedEvent, Boolean> event = bq.poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getRight()).isFalse();
    assertThat(bq.isEmpty());
    exec.shutdown();
    exec.awaitTermination(1, TimeUnit.MILLISECONDS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"VirtualThreadPinEventMonitorTest.synchronizedSleep1", "synchronizedSleep1"})
  @Disabled("flaky: no way to ensure the sequencing between the start of the recording stream and emitting the event")
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

    final Pair<RecordedEvent, Boolean> sleep1Event = bq.poll(1, TimeUnit.SECONDS);
    final Pair<RecordedEvent, Boolean> sleep2Event = bq.poll(1, TimeUnit.SECONDS);
    assertThat(sleep1Event).isNotNull();
    assertThat(sleep2Event).isNotNull();
    assertThat(sleep1Event.getRight()).isTrue();
    assertThat(sleep2Event.getRight()).isFalse();
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
