package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Optional;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

class NoiseNXClientHandshakeHandler extends AbstractNoiseClientHandler {

  private boolean receivedServerStaticKeyMessage = false;

  NoiseNXClientHandshakeHandler(final ECPublicKey rootPublicKey) {
    super(rootPublicKey);
  }

  @Override
  protected String getNoiseProtocolName() {
    return NoiseNXHandshakeHandler.NOISE_PROTOCOL_NAME;
  }

  @Override
  protected void startHandshake() {
    getHandshakeState().start();
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof BinaryWebSocketFrame frame) {
      try {
        // Don't process additional messages if we're just waiting to close because the handshake failed
        if (receivedServerStaticKeyMessage) {
          return;
        }

        receivedServerStaticKeyMessage = true;
        handleServerStaticKeyMessage(context, frame);

        context.pipeline().replace(this, null, new NoiseStreamHandler(getHandshakeState().split()));
        context.fireUserEventTriggered(new NoiseHandshakeCompleteEvent(Optional.empty()));
      } finally {
        frame.release();
      }
    } else {
      context.fireChannelRead(message);
    }
  }
}
