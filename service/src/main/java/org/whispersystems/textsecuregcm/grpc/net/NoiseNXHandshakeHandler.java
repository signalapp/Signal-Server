package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Optional;
import io.netty.util.ReferenceCountUtil;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

/**
 * A Noise NX handler handles the responder side of a Noise NX handshake.
 */
class NoiseNXHandshakeHandler extends AbstractNoiseHandshakeHandler {

  static final String NOISE_PROTOCOL_NAME = "Noise_NX_25519_ChaChaPoly_BLAKE2b";

  NoiseNXHandshakeHandler(final ECKeyPair ecKeyPair, final byte[] publicKeySignature) {
    super(NOISE_PROTOCOL_NAME, ecKeyPair, publicKeySignature);
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof BinaryWebSocketFrame frame) {
      try {
        handleEphemeralKeyMessage(context, frame);
      } finally {
        frame.release();
      }

      // All we need to do is accept the client's ephemeral key and send our own static keys; after that, we can consider
      // the handshake complete
      context.fireUserEventTriggered(new NoiseHandshakeCompleteEvent(Optional.empty()));
      context.pipeline().replace(NoiseNXHandshakeHandler.this, null, new NoiseStreamHandler(getHandshakeState().split()));
    } else {
      // Anything except binary WebSocket frames should have been filtered out of the pipeline by now; treat this as an
      // error
      ReferenceCountUtil.release(message);
      throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
    }
  }
}
