/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbUtil.TRANSACTION_ERRORS_COUNTER;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Metrics;
import jakarta.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

public class FaultTolerantDatabase {
  private final Database database;
  private final CircuitBreaker circuitBreaker;

  public FaultTolerantDatabase(
      final Database database,
      final String circuitBreakerName,
      @Nullable final String circuitBreakerConfigurationName) {
    this.database = database;
    this.circuitBreaker = circuitBreakerConfigurationName != null
        ? ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(circuitBreakerName, circuitBreakerConfigurationName)
        : ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(circuitBreakerName);
  }

  public <T> T run(final Function<? super Transaction, T> retryable) {
    return circuitBreaker.executeSupplier(() -> database.run(retryable));
  }

  /// Returns a cancellation-safe version of the result from [Database#runAsync(Function)]. Since the final stage
  /// of the result from [Database#runAsync(Function)] is a cleanup stage that closes the transaction, the
  /// transaction leaks if the future gets cancelled and the cleanup stage is skipped. So, we add another dummy stage
  /// that serves as the cancellation target and the cleanup can proceed as normal.
  ///
  /// @param retryable the block of transaction logic to execute
  /// @param <T>       the return type of retryable
  /// @return a cancellation-safe version of the future returned from [Database#runAsync(Function)]
  public <T> CompletableFuture<T> runAsync(final Function<? super Transaction, ? extends CompletableFuture<T>> retryable,
      final FoundationDbUtil.Context context) {
    return circuitBreaker.executeCompletionStage(() -> database.runAsync(retryable)
            .whenComplete((_, throwable) -> {
              if (throwable != null && ExceptionUtils.unwrap(throwable) instanceof final FDBException fdbException) {
                Metrics.counter(TRANSACTION_ERRORS_COUNTER,
                    "context", context.getName(),
                    "code", String.valueOf(fdbException.getCode())
                ).increment();
              }
            })
            .thenApply(Function.identity()))
        .toCompletableFuture();
  }

  public <T> T read(final Function<? super ReadTransaction, T> retryable) {
    return circuitBreaker.executeSupplier(() -> database.read(retryable));
  }

  public <T> CompletableFuture<T> readAsync(
      final Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable) {
    return circuitBreaker.executeCompletionStage(() -> database.readAsync(retryable))
        .toCompletableFuture();
  }
}
