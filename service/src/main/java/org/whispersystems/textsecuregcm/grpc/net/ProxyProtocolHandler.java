/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ProtocolDetectionResult;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import java.util.List;

class ProxyProtocolHandler extends ByteToMessageDecoder {

  private static final String PROXY_PROTOCOL_DETECTED_NAME =
      name(ProxyProtocolHandler.class, "proxyProtocol");

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {

    // This does not advance the read index, so the bytes we accumulate via ByteToMessageDecoder are always forwarded
    // once we get enough (either to an HAProxyMessageDecoder, or just to the rest of the pipeline)
    final ProtocolDetectionResult<HAProxyProtocolVersion> detected = HAProxyMessageDecoder.detectProtocol(in);

    switch (detected.state()) {
      case NEEDS_MORE_DATA:
        break;
      case DETECTED:
        // There is a valid proxy-protocol header. Replace ourselves with the actual decoder (which will forward our
        // accumulated bytes to the decoder via handlerRemoved)
        Metrics.counter(PROXY_PROTOCOL_DETECTED_NAME, "detected", "true").increment();
        ctx.pipeline().replace(this, "haproxy-decoder", new HAProxyMessageDecoder());
        break;
      case INVALID:
        // No header, we can just forward any bytes we've accumulated.
        Metrics.counter(PROXY_PROTOCOL_DETECTED_NAME, "detected", "false").increment();
        ctx.pipeline().remove(this);
        break;
    }
  }


}
