/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;

public interface RateLimiter {

  void validate(String key, int amount) throws RateLimitExceededException;

  CompletionStage<Void> validateAsync(String key, int amount);

  boolean hasAvailablePermits(String key, int permits);

  CompletionStage<Boolean> hasAvailablePermitsAsync(String key, int amount);

  void clear(String key);

  CompletionStage<Void> clearAsync(String key);

  RateLimiterConfig config();

  default void validate(final String key) throws RateLimitExceededException {
    validate(key, 1);
  }

  default void validate(final UUID accountUuid) throws RateLimitExceededException {
    validate(accountUuid.toString());
  }

  default void validate(final UUID srcAccountUuid, final UUID dstAccountUuid) throws RateLimitExceededException {
    validate(srcAccountUuid.toString() + "__" + dstAccountUuid.toString());
  }

  default CompletionStage<Void> validateAsync(final String key) {
    return validateAsync(key, 1);
  }

  default CompletionStage<Void> validateAsync(final UUID accountUuid) {
    return validateAsync(accountUuid.toString());
  }

  default CompletionStage<Void> validateAsync(final UUID srcAccountUuid, final UUID dstAccountUuid) {
    return validateAsync(srcAccountUuid.toString() + "__" + dstAccountUuid.toString());
  }

  default boolean hasAvailablePermits(final UUID accountUuid, final int permits) {
    return hasAvailablePermits(accountUuid.toString(), permits);
  }

  default CompletionStage<Boolean> hasAvailablePermitsAsync(final UUID accountUuid, final int permits) {
    return hasAvailablePermitsAsync(accountUuid.toString(), permits);
  }

  default void clear(final UUID accountUuid) {
    clear(accountUuid.toString());
  }

  default CompletionStage<Void> clearAsync(final UUID accountUuid) {
    return clearAsync(accountUuid.toString());
  }

  /**
   * If the wrapped {@code validate()} call throws a {@link RateLimitExceededException}, it will adapt it to ensure that
   * {@link RateLimitExceededException#isLegacy()} returns {@code true}
   */
  static void adaptLegacyException(final RateLimitValidator validator) throws RateLimitExceededException {
    try {
      validator.validate();
    } catch (final RateLimitExceededException e) {
      throw new RateLimitExceededException(e.getRetryDuration().orElse(null), false);
    }
  }

  @FunctionalInterface
  interface RateLimitValidator {

    void validate() throws RateLimitExceededException;
  }
}
