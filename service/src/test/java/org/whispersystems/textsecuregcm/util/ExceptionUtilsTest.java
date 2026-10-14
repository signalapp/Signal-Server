/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExceptionUtilsTest {

  @ParameterizedTest
  @MethodSource
  void isBreakerOpen(final Throwable t, final boolean expected) {
    assertEquals(expected, ExceptionUtils.isBreakerOpen(t));
  }

  static Collection<Arguments> isBreakerOpen() {
    final CircuitBreaker breaker = CircuitBreaker.ofDefaults("test");
    final CallNotPermittedException callNotPermittedException =
        CallNotPermittedException.createCallNotPermittedException(breaker);
    return List.of(
        Arguments.argumentSet("simple - present", callNotPermittedException, true),
        Arguments.argumentSet("nested - present", new CompletionException(callNotPermittedException), true),
        Arguments.argumentSet("deep - present", new CompletionException(new IOException(callNotPermittedException)),
            true),

        Arguments.argumentSet("null", null, false),
        Arguments.argumentSet("simple - absent", new RuntimeException(), false),
        Arguments.argumentSet("nested - absent", new CompletionException(new RuntimeException()), false),
        Arguments.argumentSet("deep - absent", new CompletionException(new IOException(new RuntimeException())), false)
    );
  }
}
