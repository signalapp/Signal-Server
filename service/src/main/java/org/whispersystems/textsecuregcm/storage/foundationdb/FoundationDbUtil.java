package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class FoundationDbUtil {

  /// Returns a cancellation-safe version of the result from [Database#runAsync(Function)]. Since the final stage
  /// of the result from [Database#runAsync(Function)] is a cleanup stage that closes the transaction, the
  /// transaction leaks if the future gets cancelled and the cleanup stage is skipped. So, we add another dummy stage
  /// that serves as the cancellation target and the cleanup can proceed as normal.
  ///
  /// @param database        the FoundationDB database instance
  /// @param retryable the block of transaction logic to execute
  /// @param <T>       the return type of retryable
  /// @return a cancellation-safe version of the future returned from [Database#runAsync(Function)]
  public static <T> CompletableFuture<T> safeRunAsync(final Database database,
      final Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
    return database.runAsync(retryable)
        .thenApply(Function.identity());
  }
}
