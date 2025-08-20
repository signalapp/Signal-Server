package org.whispersystems.textsecuregcm.grpc.net.websocket;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.net.OutboundCloseErrorMessage;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/**
 * Converts {@link OutboundCloseErrorMessage}s written to the pipeline into WebSocket close frames
 */
class WebSocketOutboundErrorHandler extends ChannelDuplexHandler {
  private static String SERVER_CLOSE_COUNTER_NAME = MetricsUtil.name(WebSocketOutboundErrorHandler.class, "serverClose");

  private boolean websocketHandshakeComplete = false;

  private static final Logger log = LoggerFactory.getLogger(WebSocketOutboundErrorHandler.class);

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
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof OutboundCloseErrorMessage err) {
      if (websocketHandshakeComplete) {
        final int status = switch (err.code()) {
          case SERVER_CLOSED -> WebSocketCloseStatus.SERVICE_RESTART.code();
          case NOISE_ERROR -> ApplicationWebSocketCloseReason.NOISE_ENCRYPTION_ERROR.getStatusCode();
          case NOISE_HANDSHAKE_ERROR -> ApplicationWebSocketCloseReason.NOISE_HANDSHAKE_ERROR.getStatusCode();
          case INTERNAL_SERVER_ERROR -> WebSocketCloseStatus.INTERNAL_SERVER_ERROR.code();
        };
        ctx.write(new CloseWebSocketFrame(new WebSocketCloseStatus(status, err.message())), promise)
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      } else {
        log.debug("Error {} occurred before websocket handshake complete", err);
        // We haven't completed a websocket handshake, so we can't really communicate errors in a semantically-meaningful
        // way; just close the connection instead.
        ctx.close();
      }
    } else {
      ctx.write(msg, promise);
    }
  }
}
