package org.whispersystems.textsecuregcm.util;

import java.util.concurrent.CompletionException;
import java.util.function.Function;

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
   * @return the first entity in the given {@code throwable}'s causal chain that is not a {@code CompletionException}
   */
  public static Throwable unwrap(Throwable throwable) {
    while (throwable instanceof CompletionException e && throwable.getCause() != null) {
      throwable = e.getCause();
    }
    return throwable;
  }

  /**
   * Wraps the given {@code throwable} in a {@link CompletionException} unless the given {@code throwable} is already a
   * {@code CompletionException}, in which case this method returns the original throwable.
   *
   * @param throwable the throwable to wrap in a {@code CompletionException}
   */
  public static CompletionException wrap(final Throwable throwable) {
    return throwable instanceof CompletionException completionException
        ? completionException
        : new CompletionException(throwable);
  }

  /**
   * Create a handler suitable for use with {@link java.util.concurrent.CompletionStage#exceptionally} that only handles
   * a specific exception subclass.
   *
   * @param exceptionType The class of exception that will be handled
   * @param fn            A function that handles exceptions of type exceptionType
   * @param <T>           The type of the stage that will be mapped
   * @param <E>           The type of the exception that will be handled
   * @return A function suitable for use with {@link java.util.concurrent.CompletionStage#exceptionally}
   */
  public static <T, E extends Throwable> Function<Throwable, ? extends T> exceptionallyHandler(
      final Class<E> exceptionType,
      final Function<E, ? extends T> fn) {
    return anyException -> {
      if (exceptionType.isInstance(anyException)) {
        return fn.apply(exceptionType.cast(anyException));
      }
      final Throwable unwrap = unwrap(anyException);
      if (exceptionType.isInstance(unwrap)) {
        return fn.apply(exceptionType.cast(unwrap));
      }
      throw wrap(anyException);
    };
  }

  /**
   * Create a handler suitable for use with {@link java.util.concurrent.CompletionStage#exceptionally} that converts
   * exceptions of a specific type to another type.
   *
   * @param exceptionType The class of exception that will be handled
   * @param fn            A function that marshals exceptions of type E to type F
   * @param <T>           The type of the stage that will be mapped
   * @param <E>           The type of the exception that will be handled
   * @param <F>           The type of the exception that will be produced
   * @return A function suitable for use with {@link java.util.concurrent.CompletionStage#exceptionally}
   */
  public static <T, E extends Throwable, F extends Throwable> Function<Throwable, ? extends T> marshal(
      final Class<E> exceptionType,
      final Function<E, F> fn) {
    return exceptionallyHandler(exceptionType, e -> {
      throw wrap(fn.apply(e));
    });
  }
}
