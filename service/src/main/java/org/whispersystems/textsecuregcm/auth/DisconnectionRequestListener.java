/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.Collection;
import java.util.UUID;

/**
 * A disconnection request listener receives and handles requests to close authenticated client network connections.
 */
public interface DisconnectionRequestListener {

  /**
   * Handles a request to close authenticated network connections for one or more authenticated devices. Requests are
   * dispatched on dedicated threads, and implementations may safely block.
   *
   * @param accountIdentifier the account identifier for which to close authenticated connections
   * @param deviceIds the device IDs within the identified account for which to close authenticated connections
   */
  void handleDisconnectionRequest(UUID accountIdentifier, Collection<Byte> deviceIds);
}
