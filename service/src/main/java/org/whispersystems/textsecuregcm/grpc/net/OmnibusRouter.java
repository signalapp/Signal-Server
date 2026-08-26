/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import java.net.SocketAddress;
import java.util.Map;

public class OmnibusRouter {

  private final Map<String, SocketAddress> routes;
  private final SocketAddress defaultBackend;

  public OmnibusRouter(final Map<String, SocketAddress> routes, final SocketAddress defaultBackend) {
    this.routes = routes;
    this.defaultBackend = defaultBackend;
  }

  SocketAddress match(final String fullPath) {
    final int queryIndex = fullPath.indexOf('?');
    final String path = queryIndex >= 0 ? fullPath.substring(0, queryIndex) : fullPath;
    return routes.getOrDefault(path, defaultBackend);
  }
}
