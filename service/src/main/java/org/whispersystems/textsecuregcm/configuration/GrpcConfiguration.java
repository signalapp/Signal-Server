/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/// Configuration for the gRPC Server
///
/// @param bindAddress      The host to bind the omnibus server to
/// @param port             The port to bind the omnibus server to
/// @param websocketAddress The address of a listening websocket server for handling legacy requests
/// @param websocketPort    The port of a listening websocket server for handling legacy requests
/// @param idleTimeout      The duration after which an idle connection may be disconnected
/// @param h2c              If true, listen for plaintext h2c with prior-knowledge
public record GrpcConfiguration(
    @NotNull String bindAddress,
    @NotNull Integer port,
    @NotNull String websocketAddress,
    @NotNull Integer websocketPort,
    @NotNull Duration idleTimeout,
    boolean h2c) {

  public GrpcConfiguration {
    if (bindAddress == null || bindAddress.isEmpty()) {
      bindAddress = "localhost";
    }
    if (websocketAddress == null || websocketAddress.isEmpty()) {
      websocketAddress = "localhost";
    }
    if (idleTimeout == null) {
      idleTimeout = Duration.ofMinutes(5);
    }
  }
}
