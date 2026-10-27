/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import java.util.Map;

public class OmnibusRouter {

  private final Map<String, H2ConnectionFactory> routesByPath;
  private final H2ConnectionFactory defaultBackend;

  public OmnibusRouter(final Map<String, H2ConnectionFactory> routesByPath, final H2ConnectionFactory defaultBackend) {
    this.routesByPath = routesByPath;
    this.defaultBackend = defaultBackend;
  }

  H2ConnectionFactory match(final String fullPath) {
    final int queryIndex = fullPath.indexOf('?');
    final String path = queryIndex >= 0 ? fullPath.substring(0, queryIndex) : fullPath;
    return routesByPath.getOrDefault(path, defaultBackend);
  }
}
