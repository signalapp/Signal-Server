package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

/**
 * A WebSocket handshake handler waits for a WebSocket handshake to complete, then replaces itself with the appropriate
 * Noise handshake handler for the requested path.
 */
class WebsocketHandshakeCompleteHandler extends ChannelInboundHandlerAdapter {

  private final ClientPublicKeysManager clientPublicKeysManager;

  private final ECKeyPair ecKeyPair;

  private final byte[] recognizedProxySecret;

  private static final Logger log = LoggerFactory.getLogger(WebsocketHandshakeCompleteHandler.class);

  @VisibleForTesting
  static final String RECOGNIZED_PROXY_SECRET_HEADER = "X-Signal-Recognized-Proxy";

  @VisibleForTesting
  static final String FORWARDED_FOR_HEADER = "X-Forwarded-For";

  WebsocketHandshakeCompleteHandler(final ClientPublicKeysManager clientPublicKeysManager,
      final ECKeyPair ecKeyPair,
      final String recognizedProxySecret) {

    this.clientPublicKeysManager = clientPublicKeysManager;
    this.ecKeyPair = ecKeyPair;

    // The recognized proxy secret is an arbitrary string, and not an encoded byte sequence (i.e. a base64- or hex-
    // encoded value). We convert it into a byte array here for easier constant-time comparisons via
    // MessageDigest.equals() later.
    this.recognizedProxySecret = recognizedProxySecret.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
    if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent) {
      final InetAddress preferredRemoteAddress;
      {
        final Optional<InetAddress> maybePreferredRemoteAddress =
            getPreferredRemoteAddress(context, handshakeCompleteEvent);

        if (maybePreferredRemoteAddress.isEmpty()) {
          context.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.INTERNAL_SERVER_ERROR,
                  "Could not determine remote address"))
              .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

          return;
        }

        preferredRemoteAddress = maybePreferredRemoteAddress.get();
      }

      GrpcClientConnectionManager.handleWebSocketHandshakeComplete(context.channel(),
          preferredRemoteAddress,
          handshakeCompleteEvent.requestHeaders().getAsString(HttpHeaderNames.USER_AGENT),
          handshakeCompleteEvent.requestHeaders().getAsString(HttpHeaderNames.ACCEPT_LANGUAGE));

      final ChannelHandler noiseHandshakeHandler = switch (handshakeCompleteEvent.requestUri()) {
        case NoiseWebSocketTunnelServer.AUTHENTICATED_SERVICE_PATH ->
            new NoiseAuthenticatedHandler(clientPublicKeysManager, ecKeyPair);

        case NoiseWebSocketTunnelServer.ANONYMOUS_SERVICE_PATH ->
            new NoiseAnonymousHandler(ecKeyPair);

        default -> {
          // The WebSocketOpeningHandshakeHandler should have caught all of these cases already; we'll consider it an
          // internal error if something slipped through.
          throw new IllegalArgumentException("Unexpected URI: " + handshakeCompleteEvent.requestUri());
        }
      };

      context.pipeline().replace(WebsocketHandshakeCompleteHandler.this, null, noiseHandshakeHandler);
    }

    context.fireUserEventTriggered(event);
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
