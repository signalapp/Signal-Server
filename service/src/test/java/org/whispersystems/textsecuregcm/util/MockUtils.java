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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomUtils;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import reactor.core.publisher.Mono;

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
      final RateLimiter mockRateLimiter,
      final String input) {
    try {
      doNothing().when(mockRateLimiter).validate(eq(input));
      doReturn(CompletableFuture.completedFuture(null)).when(mockRateLimiter).validateAsync(eq(input));
      doReturn(Mono.fromFuture(CompletableFuture.completedFuture(null))).when(mockRateLimiter).validateReactive(eq(input));
    } catch (final RateLimitExceededException e) {
      throw new RuntimeException(e);
    }
  }

  public static void updateRateLimiterResponseToAllow(
      final RateLimiter mockRateLimiter,
      final UUID input) {
    try {
      doNothing().when(mockRateLimiter).validate(eq(input));
      doReturn(CompletableFuture.completedFuture(null)).when(mockRateLimiter).validateAsync(eq(input));
      doReturn(Mono.fromFuture(CompletableFuture.completedFuture(null))).when(mockRateLimiter).validateReactive(eq(input));
    } catch (final RateLimitExceededException e) {
      throw new RuntimeException(e);
    }
  }

  public static void updateRateLimiterResponseToAllow(
      final RateLimiters rateLimitersMock,
      final RateLimiters.For handle,
      final String input) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(mockRateLimiter).when(rateLimitersMock).forDescriptor(eq(handle));
    updateRateLimiterResponseToAllow(mockRateLimiter, input);
  }

  public static void updateRateLimiterResponseToAllow(
      final RateLimiters rateLimitersMock,
      final RateLimiters.For handle,
      final UUID input) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(mockRateLimiter).when(rateLimitersMock).forDescriptor(eq(handle));
    updateRateLimiterResponseToAllow(mockRateLimiter, input);
  }

  public static Duration updateRateLimiterResponseToFail(
      final RateLimiter mockRateLimiter,
      final String input,
      final Duration retryAfter,
      final boolean legacyStatusCode) {
    try {
      final RateLimitExceededException exception = new RateLimitExceededException(retryAfter, legacyStatusCode);
      doThrow(exception).when(mockRateLimiter).validate(eq(input));
      doReturn(CompletableFuture.failedFuture(exception)).when(mockRateLimiter).validateAsync(eq(input));
      doReturn(Mono.fromFuture(CompletableFuture.failedFuture(exception))).when(mockRateLimiter).validateReactive(eq(input));
      return retryAfter;
    } catch (final RateLimitExceededException e) {
      throw new RuntimeException(e);
    }
  }

  public static Duration updateRateLimiterResponseToFail(
      final RateLimiter mockRateLimiter,
      final UUID input,
      final Duration retryAfter,
      final boolean legacyStatusCode) {
    try {
      final RateLimitExceededException exception = new RateLimitExceededException(retryAfter, legacyStatusCode);
      doThrow(exception).when(mockRateLimiter).validate(eq(input));
      doReturn(CompletableFuture.failedFuture(exception)).when(mockRateLimiter).validateAsync(eq(input));
      doReturn(Mono.fromFuture(CompletableFuture.failedFuture(exception))).when(mockRateLimiter).validateReactive(eq(input));
      return retryAfter;
    } catch (final RateLimitExceededException e) {
      throw new RuntimeException(e);
    }
  }

  public static Duration updateRateLimiterResponseToFail(
      final RateLimiters rateLimitersMock,
      final RateLimiters.For handle,
      final String input,
      final Duration retryAfter,
      final boolean legacyStatusCode) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(mockRateLimiter).when(rateLimitersMock).forDescriptor(eq(handle));
    return updateRateLimiterResponseToFail(mockRateLimiter, input, retryAfter, legacyStatusCode);
  }

  public static Duration updateRateLimiterResponseToFail(
      final RateLimiters rateLimitersMock,
      final RateLimiters.For handle,
      final UUID input,
      final Duration retryAfter,
      final boolean legacyStatusCode) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(mockRateLimiter).when(rateLimitersMock).forDescriptor(eq(handle));
    return updateRateLimiterResponseToFail(mockRateLimiter, input, retryAfter, legacyStatusCode);
  }

  public static SecretBytes randomSecretBytes(final int size) {
    return new SecretBytes(RandomUtils.nextBytes(size));
  }

  public static SecretBytes secretBytesOf(final int... byteVals) {
    final byte[] bytes = new byte[byteVals.length];
    for (int i = 0; i < byteVals.length; i++) {
      bytes[i] = (byte) byteVals[i];
    }
    return new SecretBytes(bytes);
  }
}
