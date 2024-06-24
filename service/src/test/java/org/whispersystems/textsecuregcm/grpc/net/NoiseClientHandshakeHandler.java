package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import java.util.Optional;

class NoiseClientHandshakeHandler extends ChannelInboundHandlerAdapter {

  private final NoiseClientHandshakeHelper handshakeHelper;
  private final byte[] payload;

  NoiseClientHandshakeHandler(NoiseClientHandshakeHelper handshakeHelper, final byte[] payload) {
    this.handshakeHelper = handshakeHelper;
    this.payload = payload;
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext context, final Object event) throws Exception {
    if (event instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent clientHandshakeStateEvent) {
      if (clientHandshakeStateEvent == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
        byte[] handshakeMessage = handshakeHelper.write(payload);
        context.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(handshakeMessage)))
            .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
      }
    }
    super.userEventTriggered(context, event);
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message)
      throws NoiseHandshakeException {
    if (message instanceof BinaryWebSocketFrame frame) {
      try {
        final byte[] payload = handshakeHelper.read(ByteBufUtil.getBytes(frame.content()));
        final Optional<byte[]> fastResponse = Optional.ofNullable(payload.length == 0 ? null : payload);
        context.pipeline().replace(this, null, new NoiseClientTransportHandler(handshakeHelper.split()));
        context.fireUserEventTriggered(new NoiseClientHandshakeCompleteEvent(fastResponse));
      } finally {
        frame.release();
      }
    } else {
      context.fireChannelRead(message);
    }
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) {
    handshakeHelper.destroy();
  }
}
