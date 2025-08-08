/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import java.util.UUID;

/**
 * A message availability listener handles message availability and presence events related to a client's open message
 * stream. Handler methods are run on dedicated threads and may safely perform blocking operations.
 * 
 * @see RedisMessageAvailabilityManager#handleClientConnected(UUID, byte, MessageAvailabilityListener)
 */
public interface MessageAvailabilityListener {

  /**
   * Indicates that a new message is available in the connected client's message queue.
   */
  void handleNewMessageAvailable();

  /**
   * Indicates that messages for the client have been persisted from short-term storage to long-term storage.
   */
  void handleMessagesPersisted();

  /**
   * Indicates a newer instance of this client has started reading messages and the listener should close this client's
   * underlying network connection.
   */
  void handleConflictingMessageConsumer();
}
