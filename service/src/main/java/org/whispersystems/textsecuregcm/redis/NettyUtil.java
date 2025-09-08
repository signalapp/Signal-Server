/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.resource.EpollProvider;
import java.time.Duration;

public class NettyUtil {
  static final Duration TCP_KEEPALIVE_IDLE = Duration.ofSeconds(30);
  static final Duration TCP_KEEPALIVE_INTERVAL = Duration.ofSeconds(30);
  static final Duration TCP_USER_TIMEOUT = Duration.ofSeconds(30);

  static void setSocketTimeoutsIfApplicable(final ClientOptions.Builder clientOptionsBuilder) {
    if (EpollProvider.isAvailable()) {
      // These socket options are only available with epoll native transport.
      clientOptionsBuilder.socketOptions(SocketOptions.builder()
          .keepAlive(SocketOptions.KeepAliveOptions.builder()
              .interval(TCP_KEEPALIVE_INTERVAL)
              .idle(TCP_KEEPALIVE_IDLE)
              .enable()
              .build())
          .tcpUserTimeout(SocketOptions.TcpUserTimeoutOptions.builder()
              .enable()
              .tcpUserTimeout(TCP_USER_TIMEOUT)
              .build())
          .build());
    }
  }
}
