/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.List;
import java.util.UUID;
import org.glassfish.jersey.server.ContainerRequest;
import org.whispersystems.textsecuregcm.util.Pair;

/**
 * A websocket refresh requirement provider watches for intra-request changes (e.g. to authentication status) that
 * require a websocket refresh.
 */
public interface WebsocketRefreshRequirementProvider {

  /**
   * Processes a request after filters have run and the request has been mapped to a destination controller.
   *
   * @param request the request to observe
   */
  void handleRequestStart(ContainerRequest request);

  /**
   * Processes a request after all normal request handling has been completed.
   *
   * @param request the request to observe
   * @return a list of pairs of account UUID/device ID pairs identifying websockets that need to be refreshed as a
   * result of the observed request
   */
  List<Pair<UUID, Long>> handleRequestFinished(ContainerRequest request);
}
