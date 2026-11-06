/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import io.netty.handler.ssl.SslProvider;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/// Configuration for the gRPC Server
///
/// @param bindAddress      The host to bind the omnibus server to
/// @param port             The port to bind the omnibus server to
/// @param websocketAddress The address of a listening websocket server for handling legacy requests
/// @param websocketPort    The port of a listening websocket server for handling legacy requests
/// @param idleTimeout      The duration after which a connection the client has not written to may be disconnected.
///                         Clients are expected to send some periodic write traffic (application-level or h2-level
///                         pings) to keep otherwise idle connections open
/// @param h2c              If true, listen for plaintext h2c with prior-knowledge
/// @param sslProvider      The TLS implementation used to terminate TLS. Defaults to `OPENSSL`.
public record GrpcConfiguration(
    @NotNull String bindAddress,
    @NotNull Integer port,
    @NotNull String websocketAddress,
    @NotNull Integer websocketPort,
    @NotNull Duration idleTimeout,
    boolean h2c,
    @NotNull SslProvider sslProvider) {

  public GrpcConfiguration {
    if (bindAddress == null || bindAddress.isEmpty()) {
      bindAddress = "localhost";
    }
    if (websocketAddress == null || websocketAddress.isEmpty()) {
      websocketAddress = "localhost";
    }
    if (idleTimeout == null) {
      idleTimeout = Duration.ofSeconds(60);
    }
    if (sslProvider == null) {
      sslProvider = SslProvider.OPENSSL;
    }
  }
}
