package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import javax.crypto.BadPaddingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An error handler serves as a general backstop for exceptions elsewhere in the pipeline. If the client has completed a
 * WebSocket handshake, the error handler will send appropriate WebSocket closure codes to the client in an attempt to
 * identify the problem. If the client has not completed a WebSocket handshake, the handler simply closes the
 * connection.
 */
class ErrorHandler extends ChannelInboundHandlerAdapter {

  private boolean websocketHandshakeComplete = false;

  private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

  @Override
  public void userEventTriggered(final ChannelHandlerContext context, final Object event) throws Exception {
    if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
      setWebsocketHandshakeComplete();
    }

    context.fireUserEventTriggered(event);
  }

  protected void setWebsocketHandshakeComplete() {
    this.websocketHandshakeComplete = true;
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
    if (websocketHandshakeComplete) {
      final WebSocketCloseStatus webSocketCloseStatus = switch (cause) {
        case NoiseHandshakeException e -> ApplicationWebSocketCloseReason.NOISE_HANDSHAKE_ERROR.toWebSocketCloseStatus(e.getMessage());
        case ClientAuthenticationException ignored -> ApplicationWebSocketCloseReason.CLIENT_AUTHENTICATION_ERROR.toWebSocketCloseStatus("Not authenticated");
        case BadPaddingException ignored -> ApplicationWebSocketCloseReason.NOISE_ENCRYPTION_ERROR.toWebSocketCloseStatus("Noise encryption error");
        default -> {
          log.warn("An unexpected exception reached the end of the pipeline", cause);
          yield WebSocketCloseStatus.INTERNAL_SERVER_ERROR;
        }
      };

      context.writeAndFlush(new CloseWebSocketFrame(webSocketCloseStatus))
          .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    } else {
      // We haven't completed a websocket handshake, so we can't really communicate errors in a semantically-meaningful
      // way; just close the connection instead.
      context.close();
    }
  }
}
