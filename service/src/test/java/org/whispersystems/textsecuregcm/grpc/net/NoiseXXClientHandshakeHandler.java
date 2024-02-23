package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.ShortBufferException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

class NoiseXXClientHandshakeHandler extends AbstractNoiseClientHandler {

  private final ECKeyPair ecKeyPair;

  private final UUID accountIdentifier;
  private final byte deviceId;

  private boolean receivedServerStaticKeyMessage = false;

  NoiseXXClientHandshakeHandler(final ECKeyPair ecKeyPair,
      final ECPublicKey rootPublicKey,
      final UUID accountIdentifier,
      final byte deviceId) {

    super(rootPublicKey);

    this.ecKeyPair = ecKeyPair;

    this.accountIdentifier = accountIdentifier;
    this.deviceId = deviceId;
  }

  @Override
  protected String getNoiseProtocolName() {
    return NoiseXXHandshakeHandler.NOISE_PROTOCOL_NAME;
  }

  @Override
  protected void startHandshake() {
    final HandshakeState handshakeState = getHandshakeState();

    // Noise-java derives the public key from the private key, so we just need to set the private key
    handshakeState.getLocalKeyPair().setPrivateKey(ecKeyPair.getPrivateKey().serialize(), 0);
    handshakeState.start();
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message)
      throws NoiseHandshakeException, ShortBufferException {
    if (message instanceof BinaryWebSocketFrame frame) {
      try {
        // Don't process additional messages if the handshake failed and we're just waiting to close
        if (receivedServerStaticKeyMessage) {
          return;
        }

        receivedServerStaticKeyMessage = true;
        handleServerStaticKeyMessage(context, frame);

        final ByteBuffer clientIdentityBuffer = ByteBuffer.allocate(17);
        clientIdentityBuffer.putLong(accountIdentifier.getMostSignificantBits());
        clientIdentityBuffer.putLong(accountIdentifier.getLeastSignificantBits());
        clientIdentityBuffer.put(deviceId);
        clientIdentityBuffer.flip();

        final HandshakeState handshakeState = getHandshakeState();

        // We're sending two 32-byte keys plus the client identity payload
        final byte[] staticKeyAndIdentityMessage = new byte[64 + clientIdentityBuffer.remaining()];
        handshakeState.writeMessage(
            staticKeyAndIdentityMessage, 0, clientIdentityBuffer.array(), 0, clientIdentityBuffer.remaining());

        context.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(staticKeyAndIdentityMessage)))
            .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

        context.pipeline().replace(this, null, new NoiseStreamHandler(handshakeState.split()));
        context.fireUserEventTriggered(new NoiseHandshakeCompleteEvent(Optional.empty()));
      } finally {
        frame.release();
      }
    } else {
      context.fireChannelRead(message);
    }
  }
}
