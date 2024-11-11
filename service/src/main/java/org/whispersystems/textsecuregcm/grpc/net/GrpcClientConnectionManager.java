package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.AttributeKey;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestListener;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

/**
 * A client connection manager associates a local connection to a local gRPC server with a remote connection through a
 * Noise-over-WebSocket tunnel. It provides access to metadata associated with the remote connection, including the
 * authenticated identity of the device that opened the connection (for non-anonymous connections). It can also close
 * connections associated with a given device if that device's credentials have changed and clients must reauthenticate.
 */
public class GrpcClientConnectionManager implements DisconnectionRequestListener {

  private final Map<LocalAddress, Channel> remoteChannelsByLocalAddress = new ConcurrentHashMap<>();
  private final Map<AuthenticatedDevice, List<Channel>> remoteChannelsByAuthenticatedDevice = new ConcurrentHashMap<>();

  @VisibleForTesting
  static final AttributeKey<AuthenticatedDevice> AUTHENTICATED_DEVICE_ATTRIBUTE_KEY =
      AttributeKey.valueOf(GrpcClientConnectionManager.class, "authenticatedDevice");

  @VisibleForTesting
  static final AttributeKey<InetAddress> REMOTE_ADDRESS_ATTRIBUTE_KEY =
      AttributeKey.valueOf(WebsocketHandshakeCompleteHandler.class, "remoteAddress");

  @VisibleForTesting
  static final AttributeKey<String> RAW_USER_AGENT_ATTRIBUTE_KEY =
      AttributeKey.valueOf(WebsocketHandshakeCompleteHandler.class, "rawUserAgent");

  @VisibleForTesting
  static final AttributeKey<UserAgent> PARSED_USER_AGENT_ATTRIBUTE_KEY =
      AttributeKey.valueOf(WebsocketHandshakeCompleteHandler.class, "userAgent");

  @VisibleForTesting
  static final AttributeKey<List<Locale.LanguageRange>> ACCEPT_LANGUAGE_ATTRIBUTE_KEY =
      AttributeKey.valueOf(WebsocketHandshakeCompleteHandler.class, "acceptLanguage");

  private static final Logger log = LoggerFactory.getLogger(GrpcClientConnectionManager.class);

  /**
   * Returns the authenticated device associated with the given local address, if any. An authenticated device is
   * available if and only if the given local address maps to an active local connection and that connection is
   * authenticated (i.e. not anonymous).
   *
   * @param localAddress the local address for which to find an authenticated device
   *
   * @return the authenticated device associated with the given local address, if any
   */
  public Optional<AuthenticatedDevice> getAuthenticatedDevice(final LocalAddress localAddress) {
    return getAuthenticatedDevice(remoteChannelsByLocalAddress.get(localAddress));
  }

  private Optional<AuthenticatedDevice> getAuthenticatedDevice(@Nullable final Channel remoteChannel) {
    return Optional.ofNullable(remoteChannel)
        .map(channel -> channel.attr(AUTHENTICATED_DEVICE_ATTRIBUTE_KEY).get());
  }

  /**
   * Returns the parsed acceptable languages associated with the given local address, if any. Acceptable languages may
   * be unavailable if the local connection associated with the given local address has already closed, if the client
   * did not provide a list of acceptable languages, or the list provided by the client could not be parsed.
   *
   * @param localAddress the local address for which to find acceptable languages
   *
   * @return the acceptable languages associated with the given local address, if any
   */
  public Optional<List<Locale.LanguageRange>> getAcceptableLanguages(final LocalAddress localAddress) {
    return Optional.ofNullable(remoteChannelsByLocalAddress.get(localAddress))
        .map(remoteChannel -> remoteChannel.attr(ACCEPT_LANGUAGE_ATTRIBUTE_KEY).get());
  }

  /**
   * Returns the remote address associated with the given local address, if any. A remote address may be unavailable if
   * the local connection associated with the given local address has already closed.
   *
   * @param localAddress the local address for which to find a remote address
   *
   * @return the remote address associated with the given local address, if any
   */
  public Optional<InetAddress> getRemoteAddress(final LocalAddress localAddress) {
    return Optional.ofNullable(remoteChannelsByLocalAddress.get(localAddress))
        .map(remoteChannel -> remoteChannel.attr(REMOTE_ADDRESS_ATTRIBUTE_KEY).get());
  }

  /**
   * Returns the parsed user agent provided by the client the opened the connection associated with the given local
   * address. This method may return an empty value if no active local connection is associated with the given local
   * address or if the client's user-agent string was not recognized.
   *
   * @param localAddress the local address for which to find a User-Agent string
   *
   * @return the user agent associated with the given local address
   */
  public Optional<UserAgent> getUserAgent(final LocalAddress localAddress) {
    return Optional.ofNullable(remoteChannelsByLocalAddress.get(localAddress))
        .map(remoteChannel -> remoteChannel.attr(PARSED_USER_AGENT_ATTRIBUTE_KEY).get());
  }

  /**
   * Closes any client connections to this host associated with the given authenticated device.
   *
   * @param authenticatedDevice the authenticated device for which to close connections
   */
  public void closeConnection(final AuthenticatedDevice authenticatedDevice) {
    // Channels will actually get removed from the list/map by their closeFuture listeners
    remoteChannelsByAuthenticatedDevice.getOrDefault(authenticatedDevice, Collections.emptyList()).forEach(channel ->
        channel.writeAndFlush(new CloseWebSocketFrame(ApplicationWebSocketCloseReason.REAUTHENTICATION_REQUIRED
                .toWebSocketCloseStatus("Reauthentication required")))
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE));
  }

  @VisibleForTesting
  @Nullable List<Channel> getRemoteChannelsByAuthenticatedDevice(final AuthenticatedDevice authenticatedDevice) {
    return remoteChannelsByAuthenticatedDevice.get(authenticatedDevice);
  }

  @VisibleForTesting
  Channel getRemoteChannelByLocalAddress(final LocalAddress localAddress) {
    return remoteChannelsByLocalAddress.get(localAddress);
  }

  /**
   * Handles successful completion of a WebSocket handshake and associates attributes and headers from the handshake
   * request with the channel via which the handshake took place.
   *
   * @param channel the channel that completed a WebSocket handshake
   * @param preferredRemoteAddress the preferred remote address (potentially from a request header) for the handshake
   * @param userAgentHeader the value of the User-Agent header provided in the handshake request; may be {@code null}
   * @param acceptLanguageHeader the value of the Accept-Language header provided in the handshake request; may be
   * {@code null}
   */
  static void handleWebSocketHandshakeComplete(final Channel channel,
      final InetAddress preferredRemoteAddress,
      @Nullable final String userAgentHeader,
      @Nullable final String acceptLanguageHeader) {

    channel.attr(GrpcClientConnectionManager.REMOTE_ADDRESS_ATTRIBUTE_KEY).set(preferredRemoteAddress);

    if (StringUtils.isNotBlank(userAgentHeader)) {
      channel.attr(GrpcClientConnectionManager.RAW_USER_AGENT_ATTRIBUTE_KEY).set(userAgentHeader);

      try {
        channel.attr(GrpcClientConnectionManager.PARSED_USER_AGENT_ATTRIBUTE_KEY)
            .set(UserAgentUtil.parseUserAgentString(userAgentHeader));
      } catch (final UnrecognizedUserAgentException ignored) {
      }
    }

    if (StringUtils.isNotBlank(acceptLanguageHeader)) {
      try {
        channel.attr(GrpcClientConnectionManager.ACCEPT_LANGUAGE_ATTRIBUTE_KEY).set(Locale.LanguageRange.parse(acceptLanguageHeader));
      } catch (final IllegalArgumentException e) {
        log.debug("Invalid Accept-Language header from User-Agent {}: {}", userAgentHeader, acceptLanguageHeader, e);
      }
    }
  }

  /**
   * Handles successful establishment of a Noise-over-WebSocket connection from a remote client to a local gRPC server.
   *
   * @param localChannel the newly-opened local channel between the Noise-over-WebSocket tunnel and the local gRPC
   *                     server
   * @param remoteChannel the channel from the remote client to the Noise-over-WebSocket tunnel
   * @param maybeAuthenticatedDevice the authenticated device (if any) associated with the new connection
   */
  void handleConnectionEstablished(final LocalChannel localChannel,
      final Channel remoteChannel,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<AuthenticatedDevice> maybeAuthenticatedDevice) {

    maybeAuthenticatedDevice.ifPresent(authenticatedDevice ->
        remoteChannel.attr(GrpcClientConnectionManager.AUTHENTICATED_DEVICE_ATTRIBUTE_KEY).set(authenticatedDevice));

    remoteChannelsByLocalAddress.put(localChannel.localAddress(), remoteChannel);

    getAuthenticatedDevice(remoteChannel).ifPresent(authenticatedDevice ->
        remoteChannelsByAuthenticatedDevice.compute(authenticatedDevice, (ignored, existingChannelList) -> {
          final List<Channel> channels = existingChannelList != null ? existingChannelList : new ArrayList<>();
          channels.add(remoteChannel);

          return channels;
        }));

    remoteChannel.closeFuture().addListener(closeFuture -> {
      remoteChannelsByLocalAddress.remove(localChannel.localAddress());

      getAuthenticatedDevice(remoteChannel).ifPresent(authenticatedDevice ->
          remoteChannelsByAuthenticatedDevice.compute(authenticatedDevice, (ignored, existingChannelList) -> {
            if (existingChannelList == null) {
              return null;
            }

            existingChannelList.remove(remoteChannel);

            return existingChannelList.isEmpty() ? null : existingChannelList;
          }));
    });
  }

  @Override
  public void handleDisconnectionRequest(final UUID accountIdentifier, final Collection<Byte> deviceIds) {
    deviceIds.stream()
        .map(deviceId -> new AuthenticatedDevice(accountIdentifier, deviceId))
        .forEach(this::closeConnection);
  }
}
