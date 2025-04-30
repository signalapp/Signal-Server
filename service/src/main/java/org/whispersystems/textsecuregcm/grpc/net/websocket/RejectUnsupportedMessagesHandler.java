package org.whispersystems.textsecuregcm.grpc.net.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;

/**
 * A "reject unsupported message" handler closes the channel if it receives messages it does not know how to process.
 */
public class RejectUnsupportedMessagesHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof final WebSocketFrame webSocketFrame) {
      if (webSocketFrame instanceof final TextWebSocketFrame textWebSocketFrame) {
        try {
          context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.INVALID_MESSAGE_TYPE));
        } finally {
          textWebSocketFrame.release();
        }
      } else {
        // Allow all other types of WebSocket frames
        context.fireChannelRead(webSocketFrame);
      }
    } else {
      // Discard anything that's not a WebSocket frame
      ReferenceCountUtil.release(message);
      context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.INVALID_MESSAGE_TYPE));
    }
  }
}
