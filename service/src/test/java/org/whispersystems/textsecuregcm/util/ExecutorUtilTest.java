/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class ExecutorUtilTest {

  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    this.executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    this.executor.shutdown();
    this.executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void runAllWaits() {
    final AtomicLong c = new AtomicLong(5);
    ExecutorUtil.runAll(executor, Stream
        .<Runnable>generate(() -> () -> {
          Util.sleep(1);
          c.decrementAndGet();
        })
        .limit(5)
        .toList());
    assertThat(c.get()).isEqualTo(0);
  }

  @Test
  void runAllWithException() {
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> ExecutorUtil.runAll(executor, List.of(Util.NOOP, Util.NOOP, () -> {
          throw new IllegalStateException("oof");
        })));
  }

  @Test
  void runAllEmpty() {
    assertThatNoException().isThrownBy(() -> ExecutorUtil.runAll(executor, List.of()));
  }

}
