/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Metrics;
import jakarta.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

public class FaultTolerantDatabase {
  private static final String TRANSACTION_ERRORS_COUNTER = MetricsUtil.name(FaultTolerantDatabase.class, "transactionErrors");
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

  public <T> T run(final Function<? super Transaction, T> retryable, final Context context) {
    try {
      return circuitBreaker.executeSupplier(() -> database.run(retryable));
    } catch (final Exception e) {
      if (e instanceof final FDBException fdbException) {
        Metrics.counter(TRANSACTION_ERRORS_COUNTER,
            "context", context.getName(),
            "code", String.valueOf(fdbException.getCode())
        ).increment();
      }
      throw e;
    }
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
      final Context context) {
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

  public <T> T read(final Function<? super ReadTransaction, T> retryable, final Context context) {
    try {
      return circuitBreaker.executeSupplier(() -> database.read(retryable));
    } catch (final Exception e) {
      if (e instanceof final FDBException fdbException) {
        Metrics.counter(TRANSACTION_ERRORS_COUNTER,
            "context", context.getName(),
            "code", String.valueOf(fdbException.getCode())
        ).increment();
      }
      throw e;
    }
  }

  public <T> CompletableFuture<T> readAsync(
      final Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable,
      final Context context) {
    return circuitBreaker.executeCompletionStage(() -> database.readAsync(retryable)
            .whenComplete((_, throwable) -> {
              if (throwable != null && ExceptionUtils.unwrap(throwable) instanceof final FDBException fdbException) {
                Metrics.counter(TRANSACTION_ERRORS_COUNTER,
                    "context", context.getName(),
                    "code", String.valueOf(fdbException.getCode())
                ).increment();
              }
            }))
        .toCompletableFuture();
  }

  public enum Context {
    INSERT_MESSAGE_BATCH("insertMessageBatch"),
    GET_MESSAGES_BATCH("getMessagesBatch"),
    SET_PRESENCE("setPresence"),
    GET_PRESENCE("getPresence"),
    CLEAR_PRESENCE("clearPresence"),
    CLEAR_ACCOUNT_SUBSPACE("clearAccountSubspace"),
    CLEAR_DEVICE_SUBSPACE("clearDeviceSubspace"),
    CLEAR_EXPIRED_MESSAGES("clearExpiredMessages"),
    CLEAR_EXPIRED_VERSIONSTAMPS("clearExpiredVersionstamps"),
    GET_END_OF_QUEUE("getEndOfQueue"),
    ESTIMATE_QUEUE_SIZE("estimateQueueSize"),
    ESTIMATE_QUEUE_SIZE_AND_RANGE_SPLITS("estimateQueueSizeAndRangeSplits"),
    GET_RANGE_SPLITS("getRangeSplits"),
    TRIM_QUEUE("trimQueue"),
    DELETE_MESSAGE("deleteMessage"),
    READ_ACIS("readAcis"),
    READ_STATUS("readStatus"),
    READ_VERSIONSTAMP("readVersionstamp"),
    RECORD_VERSIONSTAMP_AND_TIME("recordVersionstampAndTime"),
    TEST("test");

    private final String name;

    Context(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
