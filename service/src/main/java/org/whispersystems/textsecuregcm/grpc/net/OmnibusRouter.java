/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import java.net.SocketAddress;
import java.util.List;

public class OmnibusRouter {

  public record OmnibusRoute(String prefix, SocketAddress backend) {}

  private final List<OmnibusRoute> prefixRoutes;
  private final SocketAddress defaultBackend;

  public OmnibusRouter(final List<OmnibusRoute> prefixRoutes, final SocketAddress defaultBackend) {
    this.prefixRoutes = prefixRoutes;
    this.defaultBackend = defaultBackend;
  }

  SocketAddress match(final String path) {
    for (final OmnibusRoute route : prefixRoutes) {
      if (path.startsWith(route.prefix)) {
        return route.backend;
      }
    }
    return defaultBackend;
  }
}
