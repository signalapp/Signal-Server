package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.whispersystems.textsecuregcm.grpc.net.OutboundCloseErrorMessage;

/**
 * Translates {@link OutboundCloseErrorMessage}s into {@link NoiseDirectFrame} error frames. After error frames are
 * written, the channel is closed
 */
class NoiseDirectOutboundErrorHandler extends ChannelOutboundHandlerAdapter {

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof OutboundCloseErrorMessage err) {
      final NoiseDirectProtos.Error.Type type = switch (err.code()) {
        case SERVER_CLOSED -> NoiseDirectProtos.Error.Type.UNAVAILABLE;
        case NOISE_ERROR -> NoiseDirectProtos.Error.Type.ENCRYPTION_ERROR;
        case NOISE_HANDSHAKE_ERROR -> NoiseDirectProtos.Error.Type.HANDSHAKE_ERROR;
        case AUTHENTICATION_ERROR -> NoiseDirectProtos.Error.Type.AUTHENTICATION_ERROR;
        case INTERNAL_SERVER_ERROR -> NoiseDirectProtos.Error.Type.INTERNAL_ERROR;
      };
      final NoiseDirectProtos.Error proto = NoiseDirectProtos.Error.newBuilder()
          .setType(type)
          .setMessage(err.message())
          .build();
      final ByteBuf byteBuf = ctx.alloc().buffer(proto.getSerializedSize());
      proto.writeTo(new ByteBufOutputStream(byteBuf));
      ctx.writeAndFlush(new NoiseDirectFrame(NoiseDirectFrame.FrameType.ERROR, byteBuf))
          .addListener(ChannelFutureListener.CLOSE);
    } else {
      ctx.write(msg, promise);
    }
  }
}
