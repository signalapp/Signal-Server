package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

class InboundCloseWebSocketFrameHandler extends ChannelInboundHandlerAdapter {

  private final WebSocketCloseListener webSocketCloseListener;

  public InboundCloseWebSocketFrameHandler(final WebSocketCloseListener webSocketCloseListener) {
    this.webSocketCloseListener = webSocketCloseListener;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof CloseWebSocketFrame closeWebSocketFrame) {
      webSocketCloseListener.handleWebSocketClosedByServer(closeWebSocketFrame.statusCode());
    }

    super.channelRead(context, message);
  }
}
