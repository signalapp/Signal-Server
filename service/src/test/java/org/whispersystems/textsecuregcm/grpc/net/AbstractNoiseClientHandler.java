package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

abstract class AbstractNoiseClientHandler extends ChannelInboundHandlerAdapter {

  private final ECPublicKey rootPublicKey;

  private final HandshakeState handshakeState;

  AbstractNoiseClientHandler(final ECPublicKey rootPublicKey) {
    this.rootPublicKey = rootPublicKey;

    try {
      handshakeState = new HandshakeState(getNoiseProtocolName(), HandshakeState.INITIATOR);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Unsupported Noise algorithm: " + getNoiseProtocolName(), e);
    }
  }

  protected abstract String getNoiseProtocolName();

  protected abstract void startHandshake();

  protected HandshakeState getHandshakeState() {
    return handshakeState;
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext context, final Object event) throws Exception {
    if (event instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent clientHandshakeStateEvent) {
      if (clientHandshakeStateEvent == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
        startHandshake();

        final byte[] ephemeralKeyMessage = new byte[32];
        handshakeState.writeMessage(ephemeralKeyMessage, 0, null, 0, 0);

        context.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(ephemeralKeyMessage)))
            .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
      }
    }

    super.userEventTriggered(context, event);
  }

  protected void handleServerStaticKeyMessage(final ChannelHandlerContext context, final BinaryWebSocketFrame frame)
      throws NoiseHandshakeException {

    // The frame is coming right off the wire and so will be a direct buffer not backed by an array; copy it to a heap
    // buffer so we can Noise at it.
    final ByteBuf keyMaterialBuffer = context.alloc().heapBuffer(frame.content().readableBytes());
    final byte[] serverPublicKeySignature = new byte[64];

    try {
      frame.content().readBytes(keyMaterialBuffer);

      final int payloadBytesRead =
          handshakeState.readMessage(keyMaterialBuffer.array(), keyMaterialBuffer.arrayOffset(), keyMaterialBuffer.readableBytes(), serverPublicKeySignature, 0);

      if (payloadBytesRead != 64) {
        throw new NoiseHandshakeException("Unexpected signature length");
      }
    } catch (final ShortBufferException e) {
      throw new NoiseHandshakeException("Unexpected signature length");
    } catch (final BadPaddingException e) {
      throw new NoiseHandshakeException("Invalid keys");
    } finally {
      keyMaterialBuffer.release();
    }

    final byte[] serverPublicKey = new byte[32];
    handshakeState.getRemotePublicKey().getPublicKey(serverPublicKey, 0);

    if (!rootPublicKey.verifySignature(serverPublicKey, serverPublicKeySignature)) {
      throw new NoiseHandshakeException("Invalid server public key signature");
    }
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) throws Exception {
    handshakeState.destroy();
  }
}
