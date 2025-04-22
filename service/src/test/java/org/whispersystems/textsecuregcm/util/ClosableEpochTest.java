/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class ClosableEpochTest {

  @Test
  void close() {
    {
      final AtomicBoolean closed = new AtomicBoolean(false);
      final ClosableEpoch closableEpoch = new ClosableEpoch(() -> closed.set(true));

      assertTrue(closableEpoch.tryArrive(), "New callers should be allowed to arrive before closure");
      assertEquals(1, closableEpoch.getActiveCallers());

      closableEpoch.close();
      assertFalse(closableEpoch.tryArrive(), "New callers should not be allowed to arrive after closure");
      assertEquals(1, closableEpoch.getActiveCallers());
      assertFalse(closed.get(), "Close handler should not fire until all callers have departed");

      closableEpoch.depart();
      assertTrue(closed.get(), "Close handler should fire after last caller departs");
      assertEquals(0, closableEpoch.getActiveCallers());

      assertThrows(IllegalStateException.class, closableEpoch::close,
          "Double-closing a epoch should throw an exception");
    }

    {
      final AtomicBoolean closed = new AtomicBoolean(false);
      final ClosableEpoch closableEpoch = new ClosableEpoch(() -> closed.set(true));

      closableEpoch.close();
      assertTrue(closed.get(), "Empty epoch should fire close handler immediately on closure");
      assertEquals(0, closableEpoch.getActiveCallers());
    }
  }

  @Test
  @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void closeConcurrent() throws InterruptedException {
    final AtomicBoolean closed = new AtomicBoolean(false);
    final ClosableEpoch closableEpoch = new ClosableEpoch(() -> {
      synchronized (closed) {
        closed.set(true);
        closed.notifyAll();
      }
    });

    final int threadCount = 128;
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount);

    // Spawn a bunch of threads doing some simulated work. Close the epoch roughly halfway through. Some threads should
    // successfully enter the critical section and others should be rejected.
    for (int t = 0; t < threadCount; t++) {
      final boolean shouldClose = t == threadCount / 2;

      Thread.ofVirtual().start(() -> {
        try {
          // Wait for all threads to reach the proverbial starting line
          cyclicBarrier.await();
        } catch (final InterruptedException | BrokenBarrierException ignored) {
        }

        if (shouldClose) {
          closableEpoch.close();
        }

        if (closableEpoch.tryArrive()) {
          // Perform some simulated "work"
          try {
            Thread.sleep(1);
          } catch (final InterruptedException ignored) {
          } finally {
            closableEpoch.depart();
          }
        }
      });
    }

    while (!closed.get()) {
      synchronized (closed) {
        closed.wait();
      }
    }

    assertEquals(0, closableEpoch.getActiveCallers());
  }
}
