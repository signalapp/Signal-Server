package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Grpc;
import io.grpc.ServerCall;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
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
import org.whispersystems.textsecuregcm.grpc.ChannelNotFoundException;
import org.whispersystems.textsecuregcm.grpc.RequestAttributes;
import org.whispersystems.textsecuregcm.util.ClosableEpoch;

/**
 * A client connection manager associates a local connection to a local gRPC server with a remote connection through a
 * Noise tunnel. It provides access to metadata associated with the remote connection, including the authenticated
 * identity of the device that opened the connection (for non-anonymous connections). It can also close connections
 * associated with a given device if that device's credentials have changed and clients must reauthenticate.
 * <p>
 * In general, all {@link ServerCall}s <em>must</em> have a local address that in turn <em>should</em> be resolvable to
 * a remote channel, which <em>must</em> have associated request attributes and authentication status. It is possible
 * that a server call's local address may not be resolvable to a remote channel if the remote channel closed in the
 * narrow window between a server call being created and the start of call execution, in which case accessor methods
 * in this class will throw a {@link ChannelNotFoundException}.
 * <p>
 * A gRPC client connection manager's methods for getting request attributes accept {@link ServerCall} entities to
 * identify connections. In general, these methods should only be called from {@link io.grpc.ServerInterceptor}s.
 * Methods for requesting connection closure accept an {@link AuthenticatedDevice} to identify the connection and may
 * be called from any application code.
 */
public class GrpcClientConnectionManager implements DisconnectionRequestListener {

  private final Map<LocalAddress, Channel> remoteChannelsByLocalAddress = new ConcurrentHashMap<>();
  private final Map<AuthenticatedDevice, List<Channel>> remoteChannelsByAuthenticatedDevice = new ConcurrentHashMap<>();

  @VisibleForTesting
  static final AttributeKey<AuthenticatedDevice> AUTHENTICATED_DEVICE_ATTRIBUTE_KEY =
      AttributeKey.valueOf(GrpcClientConnectionManager.class, "authenticatedDevice");

  @VisibleForTesting
  public static final AttributeKey<RequestAttributes> REQUEST_ATTRIBUTES_KEY =
      AttributeKey.valueOf(GrpcClientConnectionManager.class, "requestAttributes");

  @VisibleForTesting
  static final AttributeKey<ClosableEpoch> EPOCH_ATTRIBUTE_KEY =
      AttributeKey.valueOf(GrpcClientConnectionManager.class, "epoch");

  private static OutboundCloseErrorMessage SERVER_CLOSED =
      new OutboundCloseErrorMessage(OutboundCloseErrorMessage.Code.SERVER_CLOSED, "server closed");

  private static final Logger log = LoggerFactory.getLogger(GrpcClientConnectionManager.class);

  /**
   * Returns the authenticated device associated with the given server call, if any. If the connection is anonymous
   * (i.e. unauthenticated), the returned value will be empty.
   *
   * @param serverCall the gRPC server call for which to find an authenticated device
   *
   * @return the authenticated device associated with the given local address, if any
   *
   * @throws ChannelNotFoundException if the server call is not associated with a known channel; in practice, this
   * generally indicates that the channel has closed while request processing is still in progress
   */
  public Optional<AuthenticatedDevice> getAuthenticatedDevice(final ServerCall<?, ?> serverCall)
      throws ChannelNotFoundException {

    return getAuthenticatedDevice(getRemoteChannel(serverCall));
  }

  @VisibleForTesting
  Optional<AuthenticatedDevice> getAuthenticatedDevice(final Channel remoteChannel) {
    return Optional.ofNullable(remoteChannel.attr(AUTHENTICATED_DEVICE_ATTRIBUTE_KEY).get());
  }

  /**
   * Returns the request attributes associated with the given server call.
   *
   * @param serverCall the gRPC server call for which to retrieve request attributes
   *
   * @return the request attributes associated with the given server call
   *
   * @throws ChannelNotFoundException if the server call is not associated with a known channel; in practice, this
   * generally indicates that the channel has closed while request processing is still in progress
   */
  public RequestAttributes getRequestAttributes(final ServerCall<?, ?> serverCall) throws ChannelNotFoundException {
    return getRequestAttributes(getRemoteChannel(serverCall));
  }

  @VisibleForTesting
  RequestAttributes getRequestAttributes(final Channel remoteChannel) {
    final RequestAttributes requestAttributes = remoteChannel.attr(REQUEST_ATTRIBUTES_KEY).get();

    if (requestAttributes == null) {
      throw new IllegalStateException("Channel does not have request attributes");
    }

    return requestAttributes;
  }

  /**
   * Handles the start of a server call, incrementing the active call count for the remote channel associated with the
   * given server call.
   *
   * @param serverCall the server call to start
   *
   * @return {@code true} if the call should start normally or {@code false} if the call should be aborted because the
   * underlying channel is closing
   */
  public boolean handleServerCallStart(final ServerCall<?, ?> serverCall) {
    try {
      return getRemoteChannel(serverCall).attr(EPOCH_ATTRIBUTE_KEY).get().tryArrive();
    } catch (final ChannelNotFoundException e) {
      // This would only happen if the channel had already closed, which is certainly possible. In this case, the call
      // should certainly not proceed.
      return false;
    }
  }

  /**
   * Handles completion (successful or not) of a server call, decrementing the active call count for the remote channel
   * associated with the given server call.
   *
   * @param serverCall the server call to complete
   */
  public void handleServerCallComplete(final ServerCall<?, ?> serverCall) {
    try {
      getRemoteChannel(serverCall).attr(EPOCH_ATTRIBUTE_KEY).get().depart();
    } catch (final ChannelNotFoundException ignored) {
      // In practice, we'd only get here if the channel has already closed, so we can just ignore the exception
    }
  }

  /**
   * Closes any client connections to this host associated with the given authenticated device.
   *
   * @param authenticatedDevice the authenticated device for which to close connections
   */
  public void closeConnection(final AuthenticatedDevice authenticatedDevice) {
    // Channels will actually get removed from the list/map by their closeFuture listeners. We copy the list to avoid
    // concurrent modification; it's possible (though practically unlikely) that a channel can close and remove itself
    // from the list while we're still iterating, resulting in a `ConcurrentModificationException`.
    final List<Channel> channelsToClose =
        new ArrayList<>(remoteChannelsByAuthenticatedDevice.getOrDefault(authenticatedDevice, Collections.emptyList()));

    channelsToClose.forEach(channel -> channel.attr(EPOCH_ATTRIBUTE_KEY).get().close());
  }

  private static void closeRemoteChannel(final Channel channel) {
    channel.writeAndFlush(SERVER_CLOSED).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @VisibleForTesting
  @Nullable List<Channel> getRemoteChannelsByAuthenticatedDevice(final AuthenticatedDevice authenticatedDevice) {
    return remoteChannelsByAuthenticatedDevice.get(authenticatedDevice);
  }

  private Channel getRemoteChannel(final ServerCall<?, ?> serverCall) throws ChannelNotFoundException {
    return getRemoteChannel(getLocalAddress(serverCall));
  }

  @VisibleForTesting
  Channel getRemoteChannel(final LocalAddress localAddress) throws ChannelNotFoundException {
    final Channel remoteChannel = remoteChannelsByLocalAddress.get(localAddress);

    if (remoteChannel == null) {
      throw new ChannelNotFoundException();
    }

    return remoteChannelsByLocalAddress.get(localAddress);
  }

  private static LocalAddress getLocalAddress(final ServerCall<?, ?> serverCall) {
    // In this server, gRPC's "remote" channel is actually a local channel that proxies to a distinct Noise channel.
    // The gRPC "remote" address is the "local address" for the proxy connection, and the local address uniquely maps to
    // a proxied Noise channel.
    if (!(serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR) instanceof LocalAddress localAddress)) {
      throw new IllegalArgumentException("Unexpected channel type: " + serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
    }

    return localAddress;
  }

  /**
   * Handles receipt of a handshake message and associates attributes and headers from the handshake
   * request with the channel via which the handshake took place.
   *
   * @param channel the channel where the handshake was initiated
   * @param preferredRemoteAddress the preferred remote address (potentially from a request header) for the handshake
   * @param userAgentHeader the value of the User-Agent header provided in the handshake request; may be {@code null}
   * @param acceptLanguageHeader the value of the Accept-Language header provided in the handshake request; may be
   * {@code null}
   */
  public static void handleHandshakeInitiated(final Channel channel,
      final InetAddress preferredRemoteAddress,
      @Nullable final String userAgentHeader,
      @Nullable final String acceptLanguageHeader) {

    @Nullable List<Locale.LanguageRange> acceptLanguages = Collections.emptyList();

    if (StringUtils.isNotBlank(acceptLanguageHeader)) {
      try {
        acceptLanguages = Locale.LanguageRange.parse(acceptLanguageHeader);
      } catch (final IllegalArgumentException e) {
        log.debug("Invalid Accept-Language header from User-Agent {}: {}", userAgentHeader, acceptLanguageHeader, e);
      }
    }

    channel.attr(REQUEST_ATTRIBUTES_KEY)
        .set(new RequestAttributes(preferredRemoteAddress, userAgentHeader, acceptLanguages));
  }

  /**
   * Handles successful establishment of a Noise connection from a remote client to a local gRPC server.
   *
   * @param localChannel the newly-opened local channel between the Noise tunnel and the local gRPC server
   * @param remoteChannel the channel from the remote client to the Noise tunnel
   * @param maybeAuthenticatedDevice the authenticated device (if any) associated with the new connection
   */
  void handleConnectionEstablished(final LocalChannel localChannel,
      final Channel remoteChannel,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<AuthenticatedDevice> maybeAuthenticatedDevice) {

    maybeAuthenticatedDevice.ifPresent(authenticatedDevice ->
        remoteChannel.attr(GrpcClientConnectionManager.AUTHENTICATED_DEVICE_ATTRIBUTE_KEY).set(authenticatedDevice));

    remoteChannel.attr(EPOCH_ATTRIBUTE_KEY)
        .set(new ClosableEpoch(() -> closeRemoteChannel(remoteChannel)));

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
