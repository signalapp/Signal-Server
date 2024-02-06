/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

/**
 * Teach {@link org.whispersystems.websocket.ReusableAuth} how to make a deep copy of a principal (that is safe to
 * concurrently modify while the original principal is being read), and how to refresh a principal after it has been
 * potentially modified.
 *
 * @param <T> The underlying principal type
 */
public interface PrincipalSupplier<T> {

  /**
   * Re-fresh the principal after it has been modified.
   * <p>
   * If the principal is populated from a backing store, refresh should re-read it.
   *
   * @param t the potentially stale principal to refresh
   * @return The up-to-date principal
   */
  T refresh(T t);

  /**
   * Create a deep, in-memory copy of the principal. This should be identical to the original principal, but should
   * share no mutable state with the original. It should be safe for two threads to independently write and read from
   * two independent deep copies.
   *
   * @param t the principal to copy
   * @return An in-memory copy of the principal
   */
  T deepCopy(T t);

  class ImmutablePrincipalSupplier<T> implements PrincipalSupplier<T> {
    @SuppressWarnings({"rawtypes"})
    private static final PrincipalSupplier INSTANCE = new ImmutablePrincipalSupplier();

    @Override
    public T refresh(final T t) {
      return t;
    }

    @Override
    public T deepCopy(final T t) {
      return t;
    }
  }

  /**
   * @return A principal supplier that can be used if the principal type does not support modification.
   */
  static <T> PrincipalSupplier<T> forImmutablePrincipal() {
    //noinspection unchecked
    return (PrincipalSupplier<T>) ImmutablePrincipalSupplier.INSTANCE;
  }
}
