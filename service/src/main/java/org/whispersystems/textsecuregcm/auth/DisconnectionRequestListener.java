/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

/**
 * A disconnection request listener receives and handles a request to close an authenticated network connection for a
 * specific client.
 */
public interface DisconnectionRequestListener {

  /**
   * Handles a request to close an authenticated network connection for a specific authenticated device. Requests are
   * dispatched on dedicated threads, and implementations may safely block.
   */
  void handleDisconnectionRequest();
}
