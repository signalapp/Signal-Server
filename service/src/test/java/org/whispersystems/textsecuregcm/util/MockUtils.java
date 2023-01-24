/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.util.Optional;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;

public final class MockUtils {

  private MockUtils() {
    // utility class
  }

  @FunctionalInterface
  public interface MockInitializer<T> {

    void init(T mock) throws Exception;
  }

  public static <T> T buildMock(final Class<T> clazz, final MockInitializer<T> initializer) throws RuntimeException {
    final T mock = Mockito.mock(clazz);
    try {
      initializer.init(mock);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return mock;
  }

  public static MutableClock mutableClock(final long timeMillis) {
    return new MutableClock(timeMillis);
  }

  public static void updateRateLimiterResponseToAllow(
      final RateLimiters rateLimitersMock,
      final RateLimiters.Handle handle,
      final String input) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(Optional.of(mockRateLimiter)).when(rateLimitersMock).byHandle(eq(handle));
    try {
      doNothing().when(mockRateLimiter).validate(eq(input));
    } catch (final RateLimitExceededException e) {
      throw new RuntimeException(e);
    }
  }

  public static void updateRateLimiterResponseToFail(
      final RateLimiters rateLimitersMock,
      final RateLimiters.Handle handle,
      final String input,
      final Duration retryAfter,
      final boolean legacyStatusCode) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(Optional.of(mockRateLimiter)).when(rateLimitersMock).byHandle(eq(handle));
    try {
      doThrow(new RateLimitExceededException(retryAfter, legacyStatusCode)).when(mockRateLimiter).validate(eq(input));
    } catch (final RateLimitExceededException e) {
      throw new RuntimeException(e);
    }
  }
}
