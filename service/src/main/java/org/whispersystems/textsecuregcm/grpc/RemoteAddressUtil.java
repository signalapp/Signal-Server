/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import java.net.SocketAddress;

public class RemoteAddressUtil {

  static final Context.Key<SocketAddress> REMOTE_ADDRESS_CONTEXT_KEY = Context.key("remote-address");

  /**
   * Returns the socket address of the remote client in the current gRPC request context.
   *
   * @return the socket address of the remote client
   */
  public static SocketAddress getRemoteAddress() {
    return REMOTE_ADDRESS_CONTEXT_KEY.get();
  }
}
