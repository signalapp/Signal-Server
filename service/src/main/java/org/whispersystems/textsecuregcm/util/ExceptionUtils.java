package org.whispersystems.textsecuregcm.util;

import java.util.concurrent.CompletionException;

public final class ExceptionUtils {

  private ExceptionUtils() {
    // utility class
  }
  
  /**
   * Extracts the cause of a {@link CompletionException}. If the given {@code throwable} is a
   * {@code CompletionException}, this method will recursively iterate through its causal chain until it finds the first
   * cause that is not a {@code CompletionException}. If the last {@code CompletionException} in the causal chain has a
   * {@code null} cause, then this method returns the last {@code CompletionException} in the chain. If the given
   * {@code throwable} is not a {@code CompletionException}, then this method returns the original {@code throwable}.
   *
   * @param throwable the throwable to "unwrap"
   *
   * @return the first entity in the given {@code throwable}'s causal chain that is not a {@code CompletionException}
   */
  public static Throwable unwrap(Throwable throwable) {
    while (throwable instanceof CompletionException e && throwable.getCause() != null) {
      throwable = e.getCause();
    }
    return throwable;
  }

  /**
   * Wraps the given {@code throwable} in a {@link CompletionException} unless the given {@code throwable} is already
   * a {@code CompletionException}, in which case this method returns the original throwable.
   *
   * @param throwable the throwable to wrap in a {@code CompletionException}
   */
  public static CompletionException wrap(final Throwable throwable) {
    return throwable instanceof CompletionException completionException
        ? completionException
        : new CompletionException(throwable);
  }
}
