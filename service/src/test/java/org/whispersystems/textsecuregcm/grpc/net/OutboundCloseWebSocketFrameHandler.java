package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

class OutboundCloseWebSocketFrameHandler extends ChannelOutboundHandlerAdapter {

  private final WebSocketCloseListener webSocketCloseListener;

  OutboundCloseWebSocketFrameHandler(final WebSocketCloseListener webSocketCloseListener) {
    this.webSocketCloseListener = webSocketCloseListener;
  }

  @Override
  public void write(final ChannelHandlerContext context, final Object message, final ChannelPromise promise) throws Exception {
    if (message instanceof CloseWebSocketFrame closeWebSocketFrame) {
      webSocketCloseListener.handleWebSocketClosedByClient(closeWebSocketFrame.statusCode());
    }

    super.write(context, message, promise);
  }
}
