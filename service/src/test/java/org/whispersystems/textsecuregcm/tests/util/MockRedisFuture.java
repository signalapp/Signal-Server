/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import io.lettuce.core.RedisFuture;
import org.jetbrains.annotations.NotNull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class MockRedisFuture<T> extends CompletableFuture<T> implements RedisFuture<T> {

  public static <T> MockRedisFuture<T> completedFuture(final T value) {
    final MockRedisFuture<T> future = new MockRedisFuture<T>();
    future.complete(value);
    return future;
  }

  public static <U> MockRedisFuture<U> failedFuture(final Throwable cause) {
    final MockRedisFuture<U> future = new MockRedisFuture<U>();
    future.completeExceptionally(cause);
    return future;
  }

  @Override
  public String getError() {
    return null;
  }

  @Override
  public boolean await(final long l, final TimeUnit timeUnit) throws InterruptedException {
    return false;
  }
}
