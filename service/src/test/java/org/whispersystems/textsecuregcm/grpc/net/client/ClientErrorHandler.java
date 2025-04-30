package org.whispersystems.textsecuregcm.grpc.net.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientErrorHandler extends ChannelInboundHandlerAdapter {
  private static final Logger log = LoggerFactory.getLogger(ClientErrorHandler.class);

  @Override
  public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
    log.error("Caught inbound error in client; closing connection", cause);
    context.channel().close();
  }
}
