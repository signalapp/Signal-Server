/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A closable epoch is a concurrency construct that measures the number of callers in some critical section. A closable
 * epoch can be closed to prevent new callers from entering the critical section, and takes a specific action when the
 * critical section is empty after closure.
 */
public class ClosableEpoch {

  private final Runnable onCloseHandler;

  private final AtomicInteger state = new AtomicInteger();

  private static final int CLOSING_BIT_MASK = 0x00000001;

  /**
   * Constructs a new closable epoch that will execute the given handler when the epoch is closed and all callers have
   * departed the critical section. The handler will be executed on the thread that calls {@link #close()} if the
   * critical section is empty at the time of the call or on the last thread to call {@link #depart()} otherwise.
   * Callers should provide handlers that delegate execution to a specific thread/executor if more precise control over
   * which thread runs the handler is required.
   *
   * @param onCloseHandler a handler to run when the epoch is closed and all callers have departed the critical section
   */
  public ClosableEpoch(final Runnable onCloseHandler) {
    this.onCloseHandler = onCloseHandler;
  }

  /**
   * Announces the arrival of a caller at the start of the critical section. If the caller is allowed to enter the
   * critical section, the epoch's internal caller counter is incremented accordingly.
   *
   * @return {@code true} if the caller is allowed to enter the critical section or {@code false} if it is not allowed
   * to enter the critical section because this epoch is closing
   */
  public boolean tryArrive() {
    // Increment the number of active callers if and only if we're not closing. We add 2 because the lowest bit encodes
    // the "closing" state, and the bits above it encode the actual call count. More verbosely, we're doing
    // `state += (1 << 1)` to avoid overwriting the closing state bit.
    return !isClosing(state.updateAndGet(state -> isClosing(state) ? state : state + 2));
  }

  /**
   * Announces the departure of a caller from the critical section. If the epoch is closing and the caller is the last
   * to depart the critical section, then the epoch will fire its {@code onCloseHandler}.
   */
  public void depart() {
    // Decrement the active caller count unconditionally. As with `tryActive`, we work in increments of 2 to "dodge" the
    // "is closing" bit. If the call count is zero and we're closing then `state` will just have the "closing" bit set.
    if (state.addAndGet(-2) == CLOSING_BIT_MASK) {
      onCloseHandler.run();
    }
  }

  /**
   * Closes this epoch, preventing new callers from entering the critical section. If the critical section is empty when
   * this method is called, it will trigger the {@code onCloseHandler} immediately. Otherwise, the
   * {@code onCloseHandler} will fire when the last caller departs the critical section.
   *
   * @throws IllegalStateException if this epoch is already closed; note that this exception is thrown on a
   * "best-effort" basis to help callers detect bugs
   */
  public void close() {
    // Note that this is not airtight and is a "best-effort" check
    if (isClosing(state.get())) {
      throw new IllegalStateException("Epoch already closed");
    }

    // Set the "closing" bit. If the closing bit is the only bit set, then the call count is zero and we can call the
    // "on close" handler.
    if (state.updateAndGet(state -> state | CLOSING_BIT_MASK) == CLOSING_BIT_MASK) {
      onCloseHandler.run();
    }
  }

  @VisibleForTesting
  int getActiveCallers() {
    return state.get() >> 1;
  }

  private static boolean isClosing(final int state) {
    return (state & CLOSING_BIT_MASK) != 0;
  }
}
