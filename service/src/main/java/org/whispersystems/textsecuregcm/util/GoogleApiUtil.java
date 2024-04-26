package org.whispersystems.textsecuregcm.util;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class GoogleApiUtil {

  public static <T> CompletableFuture<T> toCompletableFuture(final ApiFuture<T> apiFuture, final Executor executor) {
    final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    ApiFutures.addCallback(apiFuture, new ApiFutureCallback<>() {
      @Override
      public void onSuccess(final T value) {
        completableFuture.complete(value);
      }

      @Override
      public void onFailure(final Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
      }
    }, executor);

    return completableFuture;
  }
}
