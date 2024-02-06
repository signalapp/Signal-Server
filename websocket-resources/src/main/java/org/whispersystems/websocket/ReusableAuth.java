/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket;

import java.security.Principal;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.whispersystems.websocket.auth.PrincipalSupplier;

/**
 * This class holds a principal that can be reused across requests on a websocket. Since two requests may operate
 * concurrently on the same principal, and some principals contain non thread-safe mutable state,  appropriate use of
 * this class ensures that no data races occur. It also ensures that after a principal is modified, a subsequent request
 * gets the up-to-date principal
 *
 * @param <T> The underlying principal type
 * @see PrincipalSupplier
 */
public abstract sealed class ReusableAuth<T extends Principal> {

  /**
   * Get a reference to the underlying principal that callers pledge not to modify.
   * <p>
   * The reference returned will potentially be provided to many threads concurrently accessing the principal. Callers
   * should use this method only if they can ensure that they will not modify the in-memory principal object AND they do
   * not intend to modify the underlying canonical representation of the principal.
   * <p>
   * For example, if a caller retrieves a reference to a principal, does not modify the in memory state, but updates a
   * field on a database that should be reflected in subsequent retrievals of the principal, they will have met the
   * first criteria, but not the second. In that case they should instead use {@link #mutableRef()}.
   * <p>
   * If other callers have modified the underlying principal by using {@link #mutableRef()}, this method may need to
   * refresh the principal via {@link PrincipalSupplier#refresh} which could be a blocking operation.
   *
   * @return If authenticated, a reference to the underlying principal that should not be modified
   */
  public abstract Optional<T> ref();


  public interface MutableRef<T> {

    T ref();

    void close();
  }

  /**
   * Get a reference to the underlying principal that may be modified.
   * <p>
   * The underlying principal can be safely modified. Multiple threads may operate on the same {@link ReusableAuth} so
   * long as they each have their own {@link MutableRef}. After any modifications, the caller must call
   * {@link MutableRef#close} to notify the principal has become dirty. Close should be called after modifications but
   * before sending a response on the websocket. This ensures that a request that comes in after a modification response
   * is received is guaranteed to see the modification.
   *
   * @return If authenticated, a reference to the underlying principal that may be modified
   */
  public abstract Optional<MutableRef<T>> mutableRef();

  public boolean invalidCredentialsProvided() {
    return switch (this) {
      case Invalid<T> ignored -> true;
      case ReusableAuth.Anonymous<T> ignored -> false;
      case ReusableAuth.Authenticated<T> ignored-> false;
    };
  }

  /**
   * @return A {@link ReusableAuth} indicating no credential were provided
   */
  public static <T extends Principal> ReusableAuth<T> anonymous() {
    //noinspection unchecked
    return (ReusableAuth<T>) Anonymous.ANON_RESULT;
  }

  /**
   * @return A {@link ReusableAuth} indicating that invalid credentials were provided
   */
  public static <T extends Principal> ReusableAuth<T> invalid() {
    //noinspection unchecked
    return (ReusableAuth<T>) Invalid.INVALID_RESULT;
  }

  /**
   * Create a successfully authenticated {@link ReusableAuth}
   *
   * @param principal         The authenticated principal
   * @param principalSupplier Instructions for how to refresh or copy this principal
   * @param <T>               The principal type
   * @return A {@link ReusableAuth} for a successfully authenticated principal
   */
  public static <T extends Principal> ReusableAuth<T> authenticated(T principal,
      PrincipalSupplier<T> principalSupplier) {
    return new Authenticated<>(principal, principalSupplier);
  }


  private static final class Invalid<T extends Principal> extends ReusableAuth<T> {

    @SuppressWarnings({"rawtypes"})
    private static final ReusableAuth INVALID_RESULT = new Invalid();

    @Override
    public Optional<T> ref() {
      return Optional.empty();
    }

    @Override
    public Optional<MutableRef<T>> mutableRef() {
      return Optional.empty();
    }
  }

  private static final class Anonymous<T extends Principal> extends ReusableAuth<T> {

    @SuppressWarnings({"rawtypes"})
    private static final ReusableAuth ANON_RESULT = new Anonymous();

    @Override
    public Optional<T> ref() {
      return Optional.empty();
    }

    @Override
    public Optional<MutableRef<T>> mutableRef() {
      return Optional.empty();
    }
  }

  private static final class Authenticated<T extends Principal> extends ReusableAuth<T> {

    private T basePrincipal;
    private final AtomicBoolean needRefresh = new AtomicBoolean(false);
    private final PrincipalSupplier<T> principalSupplier;

    Authenticated(final T basePrincipal, PrincipalSupplier<T> principalSupplier) {
      this.basePrincipal = basePrincipal;
      this.principalSupplier = principalSupplier;

    }

    @Override
    public Optional<T> ref() {
      maybeRefresh();
      return Optional.of(basePrincipal);
    }

    @Override
    public Optional<MutableRef<T>> mutableRef() {
      maybeRefresh();
      return Optional.of(new AuthenticatedMutableRef(principalSupplier.deepCopy(basePrincipal)));
    }

    private void maybeRefresh() {
      if (needRefresh.compareAndSet(true, false)) {
        basePrincipal = principalSupplier.refresh(basePrincipal);
      }
    }

    private class AuthenticatedMutableRef implements MutableRef<T> {

      final T ref;

      private AuthenticatedMutableRef(T ref) {
        this.ref = ref;
      }

      public T ref() {
        return ref;
      }

      public void close() {
        needRefresh.set(true);
      }
    }
  }

  private ReusableAuth() {
  }
}
