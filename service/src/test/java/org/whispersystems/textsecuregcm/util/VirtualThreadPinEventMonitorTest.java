/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import jdk.jfr.consumer.RecordedEvent;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;


public class VirtualThreadPinEventMonitorTest {

  private static void nativeMethodCall() {
    try {
      new ECPublicKey(ECKeyPair.generate().getPublicKey().serialize());
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Disabled("flaky: no way to ensure the sequencing between the start of the recording stream and emitting the event, event detection is timing based")
  public void testPinEventProduced() throws InterruptedException, ExecutionException {
    final BlockingQueue<RecordedEvent> bq = new LinkedBlockingQueue<>();
    final ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
    VirtualThreadPinEventMonitor eventMonitor = queueingLogger(exec, Set.of(), bq);
    eventMonitor.start();
    // give start a moment to begin the event stream thread
    Thread.sleep(100);

    final List<? extends Future<?>> futures = IntStream
        .range(0, 100)
        .mapToObj(ig -> exec.submit(() -> IntStream
            .range(0, 100)
            .forEach(i -> nativeMethodCall())))
        .toList();
    for (final Future<?> f : futures) {
      f.get();
    }
    Thread.sleep(1000);
    eventMonitor.stop();
    assertThat(bq.isEmpty()).isFalse();
    exec.shutdown();
    exec.awaitTermination(1, TimeUnit.MILLISECONDS);
  }


  private static VirtualThreadPinEventMonitor queueingLogger(
      final ExecutorService exec,
      final Set<String> allowedMethods,
      final BlockingQueue<RecordedEvent> bq) {
    return new VirtualThreadPinEventMonitor(exec,
        Duration.ofNanos(0),
        event -> {
          try {
            bq.put(event);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

  }
}
