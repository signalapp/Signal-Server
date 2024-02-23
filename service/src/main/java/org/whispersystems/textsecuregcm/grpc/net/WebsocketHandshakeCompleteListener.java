package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

/**
 * A WebSocket handshake listener waits for a WebSocket handshake to complete, then replaces itself with the appropriate
 * Noise handshake handler for the requested path.
 */
class WebsocketHandshakeCompleteListener extends ChannelInboundHandlerAdapter {

  private final ClientPublicKeysManager clientPublicKeysManager;

  private final ECKeyPair ecKeyPair;
  private final byte[] publicKeySignature;

  WebsocketHandshakeCompleteListener(final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair ecKeyPair,
      final byte[] publicKeySignature) {

    this.clientPublicKeysManager = clientPublicKeysManager;
    this.ecKeyPair = ecKeyPair;
    this.publicKeySignature = publicKeySignature;
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
    if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent) {
      final ChannelHandler noiseHandshakeHandler = switch (handshakeCompleteEvent.requestUri()) {
        case WebsocketNoiseTunnelServer.AUTHENTICATED_SERVICE_PATH ->
            new NoiseXXHandshakeHandler(clientPublicKeysManager, ecKeyPair, publicKeySignature);

        case WebsocketNoiseTunnelServer.ANONYMOUS_SERVICE_PATH ->
            new NoiseNXHandshakeHandler(ecKeyPair, publicKeySignature);

        default -> {
          // The HttpHandler should have caught all of these cases already; we'll consider it an internal error if
          // something slipped through.
          throw new IllegalArgumentException("Unexpected URI: " + handshakeCompleteEvent.requestUri());
        }
      };

      context.pipeline().replace(WebsocketHandshakeCompleteListener.this, null, noiseHandshakeHandler);
    }

    context.fireUserEventTriggered(event);
  }
}
