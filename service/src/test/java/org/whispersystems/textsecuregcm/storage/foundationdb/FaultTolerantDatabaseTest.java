/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.BulkheadConfiguration;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

public class FaultTolerantDatabaseTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = mock(Database.class);
  }

  @Test
  void circuitBreaker() throws InterruptedException {
    final String circuitBreakerConfigurationName = "testCircuitBreaker";
    final CircuitBreakerConfiguration circuitBreakerConfiguration = new CircuitBreakerConfiguration();

    circuitBreakerConfiguration.setSlidingWindowSize(2);
    circuitBreakerConfiguration.setSlidingWindowMinimumNumberOfCalls(2);
    circuitBreakerConfiguration.setPermittedNumberOfCallsInHalfOpenState(1);
    circuitBreakerConfiguration.setFailureRateThreshold(50);
    circuitBreakerConfiguration.setWaitDurationInOpenState(Duration.ofSeconds(1));

    ResilienceUtil.getCircuitBreakerRegistry()
        .addConfiguration(circuitBreakerConfigurationName, circuitBreakerConfiguration.toCircuitBreakerConfig());
    final FaultTolerantDatabase ftDatabase = new FaultTolerantDatabase(database, getClass().getSimpleName() + "testCircuitBreaker",
        circuitBreakerConfigurationName,
        null);

    when(database.read(any())).thenThrow(new FDBException("error", 1031));

    assertThrows(FDBException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    assertThrows(FDBException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    assertThrows(CallNotPermittedException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    Thread.sleep(1001);

    assertThrows(FDBException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    assertThrows(CallNotPermittedException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));
  }

  @Test
  void bulkhead() throws BrokenBarrierException, InterruptedException {
    final String bulkheadConfigurationName = "testBulkhead";
    final BulkheadConfiguration bulkheadConfiguration = new BulkheadConfiguration();

    final int maxConcurrentCalls = 8;

    bulkheadConfiguration.setMaxConcurrentCalls(maxConcurrentCalls);
    bulkheadConfiguration.setMaxWaitDuration(Duration.ZERO);

    ResilienceUtil.getBulkheadRegistry()
        .addConfiguration(bulkheadConfigurationName, bulkheadConfiguration.toBulkheadConfig().build());

    final FaultTolerantDatabase ftDatabase = new FaultTolerantDatabase(database, getClass().getSimpleName() + "testBulkhead",
        null,
        bulkheadConfigurationName);

    final CyclicBarrier cyclicBarrier = new CyclicBarrier(maxConcurrentCalls + 1);
    final CountDownLatch finishReadLatch = new CountDownLatch(1);

    when(database.read(any())).thenAnswer(_ -> {
      // Wait for all threads to be holding a permit
      cyclicBarrier.await();

      // Wait for the main thread to attempt and fail a read before exiting and yielding the permit
      finishReadLatch.await();

      return null;
    });

    final Thread[] threads = IntStream.range(0, maxConcurrentCalls)
        .mapToObj(_ -> new Thread(() -> ftDatabase.read(_ -> null, FaultTolerantDatabase.Context.TEST)))
        .toArray(Thread[]::new);

    for (final Thread thread : threads) {
      thread.start();
    }

    cyclicBarrier.await();

    assertThrows(BulkheadFullException.class, () -> ftDatabase.read(_ -> null, FaultTolerantDatabase.Context.TEST));

    finishReadLatch.countDown();

    for (final Thread thread : threads) {
      thread.join();
    }
  }

  @Test
  void bulkheadAsync() throws BrokenBarrierException, InterruptedException {
    final String bulkheadConfigurationName = "testBulkhead";
    final BulkheadConfiguration bulkheadConfiguration = new BulkheadConfiguration();

    final int maxConcurrentCalls = 8;

    bulkheadConfiguration.setMaxConcurrentCalls(maxConcurrentCalls);
    bulkheadConfiguration.setMaxWaitDuration(Duration.ZERO);

    ResilienceUtil.getBulkheadRegistry()
        .addConfiguration(bulkheadConfigurationName, bulkheadConfiguration.toBulkheadConfig().build());

    final FaultTolerantDatabase ftDatabase = new FaultTolerantDatabase(database, getClass().getSimpleName() + "testBulkhead",
        null,
        bulkheadConfigurationName);

    final CyclicBarrier cyclicBarrier = new CyclicBarrier(maxConcurrentCalls + 1);
    final CountDownLatch finishReadLatch = new CountDownLatch(1);

    when(database.readAsync(any())).thenAnswer(_ -> CompletableFuture.runAsync(() -> {
      try {
        // Wait for all threads to be holding a permit
        cyclicBarrier.await();

        // Wait for the main thread to attempt and fail a read before exiting and yielding the permit
        finishReadLatch.await();
      } catch (final InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    }));

    final CompletableFuture<Void> readFutures = CompletableFuture.allOf(IntStream.range(0, 8)
            .mapToObj(_ -> ftDatabase.readAsync(_ -> null, FaultTolerantDatabase.Context.TEST))
                .toArray(CompletableFuture[]::new));

    cyclicBarrier.await();

    final CompletionException completionException = assertThrows(CompletionException.class,
        () -> ftDatabase.readAsync(_ -> null, FaultTolerantDatabase.Context.TEST).join());

    assertInstanceOf(BulkheadFullException.class, completionException.getCause());

    finishReadLatch.countDown();
    readFutures.join();
  }
}
