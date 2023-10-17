/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class CompletableFutureTestUtil {

  private CompletableFutureTestUtil() {
  }

  public static <T extends Throwable> void assertFailsWithCause(final Class<T> expectedCause, final CompletableFuture<?> completableFuture) {
    assertFailsWithCause(expectedCause, completableFuture, null);
  }

  public static <T extends Throwable> void assertFailsWithCause(final Class<T> expectedCause, final CompletableFuture<?> completableFuture, final String message) {
    final CompletionException completionException = assertThrows(CompletionException.class, completableFuture::join, message);
    assertTrue(ExceptionUtils.unwrap(completionException).getClass().isAssignableFrom(expectedCause), message);
  }
}
