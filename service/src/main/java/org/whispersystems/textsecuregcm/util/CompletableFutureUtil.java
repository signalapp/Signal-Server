/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class CompletableFutureUtil {

  public static <T> CompletableFuture<T> toCompletableFuture(final ListenableFuture<T> listenableFuture,
      final Executor callbackExecutor) {
    final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    Futures.addCallback(listenableFuture, new FutureCallback<T>() {
      @Override
      public void onSuccess(@Nullable final T result) {
        completableFuture.complete(result);
      }

      @Override
      public void onFailure(final Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
      }
    }, callbackExecutor);

    return completableFuture;
  }

}
