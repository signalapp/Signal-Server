package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HAProxy message handler handles decoded HAProxyMessage instances, removing itself from the pipeline once it has
 * either handled a proxy protocol message or determined that no such message is coming.
 */
public class HAProxyMessageHandler extends ChannelInboundHandlerAdapter {

  private static final Logger log = LoggerFactory.getLogger(HAProxyMessageHandler.class);

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof HAProxyMessage haProxyMessage) {
      // Some network/deployment configurations will send us a proxy protocol message, but we don't use it. We still
      // need to clear it from the pipeline to avoid confusing the TLS machinery, though.
      log.debug("Discarding HAProxy message: {}", haProxyMessage);
      haProxyMessage.release();
    } else {
      super.channelRead(context, message);
    }

    // Regardless of the type of the first message, we'll only ever receive zero or one HAProxyMessages. After the first
    // message, all others will just be "normal" messages, and our work here is done.
    context.pipeline().remove(this);
  }
}
