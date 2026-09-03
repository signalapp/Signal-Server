package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class FoundationDbUtil {

  public enum Context {
    INSERT_MESSAGE_BATCH("insertMessageBatch"),
    GET_MESSAGES_BATCH("getMessagesBatch"),
    SET_PRESENCE("setPresence"),
    CLEAR_PRESENCE("clearPresence"),
    GET_END_OF_QUEUE("getEndOfQueue"),
    ESTIMATE_QUEUE_SIZE("estimateQueueSize"),
    ESTIMATE_QUEUE_SIZE_AND_RANGE_SPLITS("estimateQueueSizeAndRangeSplits"),
    TRIM_QUEUE("trimQueue"),
    DELETE_MESSAGE("deleteMessage");

    private final String name;

    Context(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private static final String TRANSACTION_ERRORS_COUNTER = MetricsUtil.name(FoundationDbUtil.class, "transactionErrors");

  /// Returns a cancellation-safe version of the result from [Database#runAsync(Function)]. Since the final stage of the
  /// result from [Database#runAsync(Function)] is a cleanup stage that closes the transaction, the transaction leaks if
  /// the future gets cancelled and the cleanup stage is skipped. So, we add another dummy stage that serves as the
  /// cancellation target and the cleanup can proceed as normal.
  ///
  /// @param <T>       the return type of retryable
  /// @param database  the FoundationDB database instance
  /// @param retryable the block of transaction logic to execute
  /// @param context a context label for metrics purposes
  /// @return a cancellation-safe version of the future returned from [Database#runAsync(Function)]
  public static <T> CompletableFuture<T> safeRunAsync(final Database database,
      final Function<? super Transaction, ? extends CompletableFuture<T>> retryable, final Context context) {
    return database.runAsync(retryable)
        .whenComplete((_, throwable) -> {
          if (throwable != null && ExceptionUtils.unwrap(throwable) instanceof final FDBException fdbException) {
            Metrics.counter(TRANSACTION_ERRORS_COUNTER,
                "context", context.getName(),
                "code", String.valueOf(fdbException.getCode())
            ).increment();
          }
        })
        .thenApply(Function.identity());
  }
}
