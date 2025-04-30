package org.whispersystems.textsecuregcm.grpc.net.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import java.util.function.Supplier;

class HAProxyMessageSender extends ChannelInboundHandlerAdapter {

  private final Supplier<HAProxyMessage> messageSupplier;

  HAProxyMessageSender(final Supplier<HAProxyMessage> messageSupplier) {
    this.messageSupplier = messageSupplier;
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext context) {
    if (context.channel().isActive()) {
      context.writeAndFlush(messageSupplier.get());
    }
  }

  @Override
  public void channelActive(final ChannelHandlerContext context) {
    context.writeAndFlush(messageSupplier.get());
    context.fireChannelActive();
  }
}
