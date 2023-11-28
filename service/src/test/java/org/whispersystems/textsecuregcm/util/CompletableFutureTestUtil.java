/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

public class CompletableFutureTestUtil {

  private CompletableFutureTestUtil() {
  }

  public static <T extends Throwable> T assertFailsWithCause(final Class<T> expectedCause, final CompletableFuture<?> completableFuture) {
    return assertFailsWithCause(expectedCause, completableFuture, null);
  }

  public static <T extends Throwable> T assertFailsWithCause(final Class<T> expectedCause, final CompletableFuture<?> completableFuture, final String message) {
    final CompletionException completionException = assertThrows(CompletionException.class, completableFuture::join, message);
    final Throwable unwrapped = ExceptionUtils.unwrap(completionException);
    final String compError = "Expected failure " + expectedCause + " was " + unwrapped.getClass();
    assertTrue(unwrapped.getClass().isAssignableFrom(expectedCause), message == null ? compError : message + " : " + compError);
    return expectedCause.cast(unwrapped);
  }

  public static <T> CompletableFuture<T> almostCompletedFuture(T result) {
    return new CompletableFuture<T>().completeOnTimeout(result, 5, TimeUnit.MILLISECONDS);
  }

}
