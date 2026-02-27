/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

/// Represents a supplier of results that may throw an exception.
///
/// There is no requirement that a new or distinct result be returned each time the supplier is invoked.
@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {

  /// Gets a result, potentially throwing an exception.
  ///
  /// @return a result
  ///
  /// @throws E at the discretion of the implementation
  T get() throws E;
}
