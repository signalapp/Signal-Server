package org.whispersystems.textsecuregcm.grpc.net.websocket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.net.HandshakePattern;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeInit;

/**
 * A WebSocket handshake handler waits for a WebSocket handshake to complete, then replaces itself with the appropriate
 * Noise handshake handler for the requested path.
 */
class WebsocketHandshakeCompleteHandler extends ChannelInboundHandlerAdapter {

  private final byte[] recognizedProxySecret;

  private static final Logger log = LoggerFactory.getLogger(WebsocketHandshakeCompleteHandler.class);

  @VisibleForTesting
  static final String RECOGNIZED_PROXY_SECRET_HEADER = "X-Signal-Recognized-Proxy";

  @VisibleForTesting
  static final String FORWARDED_FOR_HEADER = "X-Forwarded-For";

  private InetAddress remoteAddress = null;
  private HandshakePattern handshakePattern = null;

  WebsocketHandshakeCompleteHandler(final String recognizedProxySecret) {

    // The recognized proxy secret is an arbitrary string, and not an encoded byte sequence (i.e. a base64- or hex-
    // encoded value). We convert it into a byte array here for easier constant-time comparisons via
    // MessageDigest.equals() later.
    this.recognizedProxySecret = recognizedProxySecret.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
    if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent) {
      final Optional<InetAddress> maybePreferredRemoteAddress =
          getPreferredRemoteAddress(context, handshakeCompleteEvent);

      if (maybePreferredRemoteAddress.isEmpty()) {
        context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.INTERNAL_SERVER_ERROR,
                "Could not determine remote address"))
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

        return;
      }

      remoteAddress = maybePreferredRemoteAddress.get();
      handshakePattern = switch (handshakeCompleteEvent.requestUri()) {
        case NoiseWebSocketTunnelServer.AUTHENTICATED_SERVICE_PATH -> HandshakePattern.IK;
        case NoiseWebSocketTunnelServer.ANONYMOUS_SERVICE_PATH -> HandshakePattern.NK;
        // The WebSocketOpeningHandshakeHandler should have caught all of these cases already; we'll consider it an
        // internal error if something slipped through.
        default -> throw new IllegalArgumentException("Unexpected URI: " + handshakeCompleteEvent.requestUri());
      };
    }

    context.fireUserEventTriggered(event);
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object msg) {
    try {
      if (!(msg instanceof ByteBuf frame)) {
        throw new IllegalStateException("Unexpected msg type: " + msg.getClass());
      }

      if (handshakePattern == null || remoteAddress == null) {
        throw new IllegalStateException("Received payload before websocket handshake complete");
      }

      final NoiseHandshakeInit handshakeMessage =
          new NoiseHandshakeInit(remoteAddress, handshakePattern, frame);

      context.pipeline().remove(WebsocketHandshakeCompleteHandler.class);
      context.fireChannelRead(handshakeMessage);
    } catch (Exception e) {
      ReferenceCountUtil.release(msg);
      throw e;
    }
  }

  private Optional<InetAddress> getPreferredRemoteAddress(final ChannelHandlerContext context,
      final WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent) {

    final byte[] recognizedProxySecretFromHeader =
        handshakeCompleteEvent.requestHeaders().get(RECOGNIZED_PROXY_SECRET_HEADER, "")
            .getBytes(StandardCharsets.UTF_8);

    final boolean trustForwardedFor = MessageDigest.isEqual(recognizedProxySecret, recognizedProxySecretFromHeader);

    if (trustForwardedFor && handshakeCompleteEvent.requestHeaders().contains(FORWARDED_FOR_HEADER)) {
      final String forwardedFor = handshakeCompleteEvent.requestHeaders().get(FORWARDED_FOR_HEADER);

      return getMostRecentProxy(forwardedFor).map(mostRecentProxy -> {
        try {
          return InetAddresses.forString(mostRecentProxy);
        } catch (final IllegalArgumentException e) {
          log.warn("Failed to parse forwarded-for address: {}", forwardedFor, e);
          return null;
        }
      });
    } else {
      // Either we don't trust the forwarded-for header or it's not present
      if (context.channel().remoteAddress() instanceof InetSocketAddress inetSocketAddress) {
        return Optional.of(inetSocketAddress.getAddress());
      } else {
        log.warn("Channel's remote address was not an InetSocketAddress");
        return Optional.empty();
      }
    }
  }

  /**
   * Returns the most recent proxy in a chain described by an {@code X-Forwarded-For} header.
   *
   * @param forwardedFor the value of an X-Forwarded-For header
   * @return the IP address of the most recent proxy in the forwarding chain, or empty if none was found or
   * {@code forwardedFor} was null
   * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For">X-Forwarded-For - HTTP |
   * MDN</a>
   */
  @VisibleForTesting
  static Optional<String> getMostRecentProxy(@Nullable final String forwardedFor) {
    return Optional.ofNullable(forwardedFor)
        .map(ff -> {
          final int idx = forwardedFor.lastIndexOf(',') + 1;
          return idx < forwardedFor.length()
              ? forwardedFor.substring(idx).trim()
              : null;
        })
        .filter(StringUtils::isNotBlank);
  }
}
