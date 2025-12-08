/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import reactor.core.publisher.Mono;

public interface RateLimiter {

  void validate(String key, long amount) throws RateLimitExceededException;

  CompletionStage<Void> validateAsync(String key, long amount);

  boolean hasAvailablePermits(String key, long permits);

  CompletionStage<Boolean> hasAvailablePermitsAsync(String key, long amount);

  void clear(String key);

  CompletionStage<Void> clearAsync(String key);

  RateLimiterConfig config();

  default void validate(final String key) throws RateLimitExceededException {
    validate(key, 1);
  }

  default void validate(final UUID accountUuid) throws RateLimitExceededException {
    validate(accountUuid.toString());
  }

  default void validate(final UUID accountUuid, final long permits) throws RateLimitExceededException {
    validate(accountUuid.toString(), permits);
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

  default Mono<Void> validateReactive(final String key) {
    return Mono.fromFuture(() -> validateAsync(key).toCompletableFuture());
  }

  default Mono<Void> validateReactive(final UUID accountUuid) {
    return validateReactive(accountUuid.toString());
  }

  default boolean hasAvailablePermits(final UUID accountUuid, final long permits) {
    return hasAvailablePermits(accountUuid.toString(), permits);
  }

  default CompletionStage<Boolean> hasAvailablePermitsAsync(final UUID accountUuid, final long permits) {
    return hasAvailablePermitsAsync(accountUuid.toString(), permits);
  }

  default void clear(final UUID accountUuid) {
    clear(accountUuid.toString());
  }

  default CompletionStage<Void> clearAsync(final UUID accountUuid) {
    return clearAsync(accountUuid.toString());
  }
}
