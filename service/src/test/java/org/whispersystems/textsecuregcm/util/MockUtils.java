/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.internal.exceptions.Reporter.noMoreInteractionsWanted;
import static org.mockito.internal.invocation.InvocationsFinder.findFirstUnverified;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.mockito.invocation.MatchableInvocation;
import org.mockito.verification.VerificationMode;
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
      final Duration retryAfter) {
    try {
      final RateLimitExceededException exception = new RateLimitExceededException(retryAfter);
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
      final Duration retryAfter) {
    try {
      final RateLimitExceededException exception = new RateLimitExceededException(retryAfter);
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
      final Duration retryAfter) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(mockRateLimiter).when(rateLimitersMock).forDescriptor(eq(handle));
    return updateRateLimiterResponseToFail(mockRateLimiter, input, retryAfter);
  }

  public static Duration updateRateLimiterResponseToFail(
      final RateLimiters rateLimitersMock,
      final RateLimiters.For handle,
      final UUID input,
      final Duration retryAfter) {
    final RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    doReturn(mockRateLimiter).when(rateLimitersMock).forDescriptor(eq(handle));
    return updateRateLimiterResponseToFail(mockRateLimiter, input, retryAfter);
  }

  public static SecretBytes randomSecretBytes(final int size) {
    return new SecretBytes(TestRandomUtil.nextBytes(size));
  }

  public static SecretBytes secretBytesOf(final int... byteVals) {
    final byte[] bytes = new byte[byteVals.length];
    for (int i = 0; i < byteVals.length; i++) {
      bytes[i] = (byte) byteVals[i];
    }
    return new SecretBytes(bytes);
  }

  /**
   * modeled after {@link org.mockito.Mockito#only()}, verifies that the matched invocation is the only invocation of
   * this method
   */
  public static VerificationMode exactly() {
    return exactly(1);
  }

  /**
   * a combination of {@link #exactly()} and {@link org.mockito.Mockito#times(int)}, verifies that
   * there are exactly N invocations of this method, and all of them match the given specification
   */
  public static VerificationMode exactly(int wantedCount) {
    return data -> {
      MatchableInvocation target = data.getTarget();
      final List<Invocation> allInvocations = data.getAllInvocations();
      List<Invocation> otherInvocations = allInvocations.stream()
          .filter(target::hasSameMethod)
          .filter(Predicate.not(target::matches))
          .toList();

      if (!otherInvocations.isEmpty()) {
        Invocation unverified = findFirstUnverified(otherInvocations);
        throw noMoreInteractionsWanted(unverified, (List) allInvocations);
      }
      Mockito.times(wantedCount).verify(data);
    };
  }

}
