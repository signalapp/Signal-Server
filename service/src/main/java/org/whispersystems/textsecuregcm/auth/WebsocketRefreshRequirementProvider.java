/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.List;
import java.util.UUID;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.whispersystems.textsecuregcm.util.Pair;

/**
 * A websocket refresh requirement provider watches for intra-request changes (e.g. to authentication status) that
 * require a websocket refresh.
 */
public interface WebsocketRefreshRequirementProvider {

  /**
   * Processes a request after filters have run and the request has been mapped to a destination controller.
   *
   * @param requestEvent the request event to observe
   */
  void handleRequestFiltered(RequestEvent requestEvent);

  /**
   * Processes a request after all normal request handling has been completed.
   *
   * @param requestEvent the request event to observe
   * @return a list of pairs of account UUID/device ID pairs identifying websockets that need to be refreshed as a
   * result of the observed request
   */
  List<Pair<UUID, Byte>> handleRequestFinished(RequestEvent requestEvent);
}
