package org.whispersystems.textsecuregcm.grpc.net.client;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.Optional;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeException;
import org.whispersystems.textsecuregcm.grpc.net.NoiseTunnelProtos;
import org.whispersystems.textsecuregcm.grpc.net.OutboundCloseErrorMessage;

public class NoiseClientHandshakeHandler extends ChannelDuplexHandler {

  private final NoiseClientHandshakeHelper handshakeHelper;

  public NoiseClientHandshakeHandler(NoiseClientHandshakeHelper handshakeHelper) {
    this.handshakeHelper = handshakeHelper;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof ByteBuf plaintextHandshakePayload) {
      final byte[] payloadBytes = ByteBufUtil.getBytes(plaintextHandshakePayload,
          plaintextHandshakePayload.readerIndex(), plaintextHandshakePayload.readableBytes(),
          false);
      final byte[] handshakeMessage = handshakeHelper.write(payloadBytes);
      ctx.write(Unpooled.wrappedBuffer(handshakeMessage), promise);
    } else {
      ctx.write(msg, promise);
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message)
      throws NoiseHandshakeException {
    if (message instanceof ByteBuf frame) {
      try {
        final byte[] payload = handshakeHelper.read(ByteBufUtil.getBytes(frame));
        final NoiseTunnelProtos.HandshakeResponse handshakeResponse =
            NoiseTunnelProtos.HandshakeResponse.parseFrom(payload);

        context.pipeline().replace(this, null, new NoiseClientTransportHandler(handshakeHelper.split()));
        context.fireUserEventTriggered(new NoiseClientHandshakeCompleteEvent(handshakeResponse));
      } catch (InvalidProtocolBufferException e) {
        throw new NoiseHandshakeException("Failed to parse handshake response");
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
