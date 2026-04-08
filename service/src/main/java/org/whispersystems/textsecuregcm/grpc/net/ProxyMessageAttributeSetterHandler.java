/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.AttributeKey;
import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Reads the decoded [HAProxyMessage], stores the source address as a channel attribute, and removes itself.
class ProxyMessageAttributeSetterHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(ProxyMessageAttributeSetterHandler.class);

  /// Attribute for the remote address extracted from a proxy protocol header
  static final AttributeKey<InetAddress> PROXY_REMOTE_ADDRESS = AttributeKey.newInstance("proxyRemoteAddress");

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (!(msg instanceof HAProxyMessage proxyMessage)) {
      ctx.pipeline().remove(this);
      ctx.fireChannelRead(msg);
      return;
    }

    try {
      final String sourceAddress = proxyMessage.sourceAddress();
      if (sourceAddress != null) {
        ctx.channel().attr(PROXY_REMOTE_ADDRESS).set(InetAddress.getByName(sourceAddress));
      } else {
        logger.warn("PROXY protocol message has no source address");
      }
    } finally {
      proxyMessage.release();
      ctx.pipeline().remove(this);
    }
  }
}
