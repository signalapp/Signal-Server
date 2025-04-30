package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A proxy handler writes all data read from one channel to another peer channel.
 */
public class ProxyHandler extends ChannelInboundHandlerAdapter {

  private final Channel peerChannel;

  public ProxyHandler(final Channel peerChannel) {
    this.peerChannel = peerChannel;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) {
    peerChannel.writeAndFlush(message)
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }
}
