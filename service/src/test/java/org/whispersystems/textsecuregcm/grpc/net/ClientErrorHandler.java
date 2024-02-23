package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;

class ClientErrorHandler extends ErrorHandler {

  @Override
  public void userEventTriggered(final ChannelHandlerContext context, final Object event) throws Exception {
    if (event instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent clientHandshakeStateEvent) {
      if (clientHandshakeStateEvent == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
        setWebsocketHandshakeComplete();
      }
    }

    super.userEventTriggered(context, event);
  }
}
