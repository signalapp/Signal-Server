package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;

/**
 * A proxy protocol detection handler watches for HAProxy PROXY protocol messages at the beginning of a TCP connection.
 * If a connection begins with a proxy message, this handler will add a {@link HAProxyMessageDecoder} to the pipeline.
 * In all cases, once this handler has determined that a connection does or does not begin with a proxy protocol
 * message, it will remove itself from the pipeline and pass any intercepted down the pipeline.
 *
 * @see <a href="https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt">The PROXY protocol</a>
 */
public class ProxyProtocolDetectionHandler extends ChannelInboundHandlerAdapter {

  private CompositeByteBuf accumulator;

  @VisibleForTesting
  static final int PROXY_MESSAGE_DETECTION_BYTES = 12;

  @Override
  public void handlerAdded(final ChannelHandlerContext context) {
    // We need at least 12 bytes to decide if a byte buffer contains a proxy protocol message. Assuming we only get
    // non-empty buffers, that means we'll need at most 12 sub-buffers to have a complete message. In virtually every
    // practical case, though, we'll be able to tell from the first packet.
    accumulator = new CompositeByteBuf(context.alloc(), false, PROXY_MESSAGE_DETECTION_BYTES);
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof ByteBuf byteBuf) {
      accumulator.addComponent(true, byteBuf);

      switch (HAProxyMessageDecoder.detectProtocol(accumulator).state()) {
        case NEEDS_MORE_DATA -> {
        }

        case INVALID -> {
          // We have enough information to determine that this connection is NOT starting with a proxy protocol message,
          // and we can just pass the accumulated bytes through
          context.fireChannelRead(accumulator);

          accumulator = null;
          context.pipeline().remove(this);
        }

        case DETECTED -> {
          // We have enough information to know that we're dealing with a proxy protocol message; add appropriate
          // handlers and pass the accumulated bytes through
          context.pipeline().addAfter(context.name(), null, new HAProxyMessageDecoder());
          context.fireChannelRead(accumulator);

          accumulator = null;
          context.pipeline().remove(this);
        }
      }
    } else {
      super.channelRead(context, message);
    }
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) {
    if (accumulator != null) {
      accumulator.release();
      accumulator = null;
    }
  }
}
