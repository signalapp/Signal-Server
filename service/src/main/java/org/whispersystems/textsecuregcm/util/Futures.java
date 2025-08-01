/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.util.concurrent.CompletionStage;
import org.apache.commons.lang3.function.TriFunction;

public class Futures {

  public static <T, U, V, R> CompletionStage<R> zipWith(
      CompletionStage<T> futureT,
      CompletionStage<U> futureU,
      CompletionStage<V> futureV,
      TriFunction<T, U, V, R> fun) {

    return futureT.thenCompose(t -> futureU.thenCombine(futureV, (u, v) -> fun.apply(t, u, v)));
  }
}
