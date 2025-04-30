package org.whispersystems.textsecuregcm.grpc.net.websocket;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

/**
 * A WebSocket opening handshake handler serves as the "front door" for the WebSocket/Noise tunnel and gracefully
 * rejects requests for anything other than a WebSocket connection to a known endpoint.
 */
class WebSocketOpeningHandshakeHandler extends ChannelInboundHandlerAdapter {

  private final String authenticatedPath;
  private final String anonymousPath;
  private final String healthCheckPath;

  WebSocketOpeningHandshakeHandler(final String authenticatedPath,
      final String anonymousPath,
      final String healthCheckPath) {

    this.authenticatedPath = authenticatedPath;
    this.anonymousPath = anonymousPath;
    this.healthCheckPath = healthCheckPath;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    if (message instanceof FullHttpRequest request) {
      boolean shouldReleaseRequest = true;

      try {
        if (request.decoderResult().isSuccess()) {
          if (HttpMethod.GET.equals(request.method())) {
            if (authenticatedPath.equals(request.uri()) || anonymousPath.equals(request.uri())) {
              if (request.headers().contains(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)) {
                // Pass the request along to the websocket handshake handler and remove ourselves from the pipeline
                shouldReleaseRequest = false;

                context.fireChannelRead(request);
                context.pipeline().remove(this);
              } else {
                closeConnectionWithStatus(context, request, HttpResponseStatus.UPGRADE_REQUIRED);
              }
            } else if (healthCheckPath.equals(request.uri())) {
              closeConnectionWithStatus(context, request, HttpResponseStatus.NO_CONTENT);
            } else {
              closeConnectionWithStatus(context, request, HttpResponseStatus.NOT_FOUND);
            }
          } else {
            closeConnectionWithStatus(context, request, HttpResponseStatus.METHOD_NOT_ALLOWED);
          }
        } else {
          closeConnectionWithStatus(context, request, HttpResponseStatus.BAD_REQUEST);
        }
      } finally {
        if (shouldReleaseRequest) {
          request.release();
        }
      }
    } else {
      // Anything except HTTP requests should have been filtered out of the pipeline by now; treat this as an error
      ReferenceCountUtil.release(message);
      throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
    }
  }

  private static void closeConnectionWithStatus(final ChannelHandlerContext context,
      final FullHttpRequest request,
      final HttpResponseStatus status) {

    context.writeAndFlush(new DefaultFullHttpResponse(request.protocolVersion(), status))
        .addListener(ChannelFutureListener.CLOSE);
  }
}
