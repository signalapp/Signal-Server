/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

/// Represents an operation that two argument and returns a result, but may throw a checked exception. Unlike most other
/// functional interfaces, `ThrowingBiFunction` is expected to operate via side-effects.
@FunctionalInterface
public interface ThrowingBiFunction<T, U, R, E extends Exception> {

  /// Performs this operation on the given arguments and returns a result.
  ///
  /// @param t the first input argument
  /// @param u the second input argument
  /// @return the result of applying the function
  /// @throws E at the implementation's discretion
  R apply(T t, U u) throws E;
}
