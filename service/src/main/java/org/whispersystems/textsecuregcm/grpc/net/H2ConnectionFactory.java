/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import java.net.SocketAddress;

/// A factory that creates h2 connections to some configured target address
public interface H2ConnectionFactory {

  /// Retrieve a h2 connection to [#address()]
  ///
  /// @param callerEventLoop The event loop of the channel making this call
  ChannelFuture getConnection(final EventLoop callerEventLoop);

  /// @return The [SocketAddress] that this factory targets
  SocketAddress address();
}
