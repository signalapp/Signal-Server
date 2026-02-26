/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

/// Represents an operation that accepts a single input argument and returns no result, but may throw a checked
/// exception. Unlike most other functional interfaces, `ThrowingConsumer` is expected to operate via side-effects.
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception> {

  /// Performs this operation on the given argument.
  ///
  /// @param t the input argument
  ///
  /// @throws E at the implementation's discretion
  void accept(T t) throws E;
}
