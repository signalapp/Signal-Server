package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.net.InetAddresses;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.grpc.ChannelNotFoundException;
import org.whispersystems.textsecuregcm.grpc.RequestAttributes;
import org.whispersystems.textsecuregcm.storage.Device;

class GrpcClientConnectionManagerTest {

  private static EventLoopGroup eventLoopGroup;

  private LocalChannel localChannel;
  private LocalChannel remoteChannel;

  private LocalServerChannel localServerChannel;

  private GrpcClientConnectionManager grpcClientConnectionManager;

  @BeforeAll
  static void setUpBeforeAll() {
    eventLoopGroup = new DefaultEventLoopGroup();
  }

  @BeforeEach
  void setUp() throws InterruptedException {
    eventLoopGroup = new DefaultEventLoopGroup();

    grpcClientConnectionManager = new GrpcClientConnectionManager();

    // We have to jump through some hoops to get "real" LocalChannel instances to test with, and so we run a trivial
    // local server to which we can open trivial local connections
    localServerChannel = (LocalServerChannel) new ServerBootstrap()
        .group(eventLoopGroup)
        .channel(LocalServerChannel.class)
        .childHandler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(final Channel channel) {
          }
        })
        .bind(new LocalAddress("test-server"))
        .await()
        .channel();

    final Bootstrap clientBootstrap = new Bootstrap()
        .group(eventLoopGroup)
        .channel(LocalChannel.class)
        .handler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(final Channel ch) {
          }
        });

    localChannel = (LocalChannel) clientBootstrap.connect(localServerChannel.localAddress()).await().channel();
    remoteChannel = (LocalChannel) clientBootstrap.connect(localServerChannel.localAddress()).await().channel();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    localChannel.close().await();
    remoteChannel.close().await();
    localServerChannel.close().await();
  }

  @AfterAll
  static void tearDownAfterAll() throws InterruptedException {
    eventLoopGroup.shutdownGracefully().await();
  }

  @ParameterizedTest
  @MethodSource
  void getAuthenticatedDevice(@SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<AuthenticatedDevice> maybeAuthenticatedDevice) {
    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, maybeAuthenticatedDevice);

    assertEquals(maybeAuthenticatedDevice,
        grpcClientConnectionManager.getAuthenticatedDevice(remoteChannel));
  }

  private static List<Optional<AuthenticatedDevice>> getAuthenticatedDevice() {
    return List.of(
        Optional.of(new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID)),
        Optional.empty()
    );
  }

  @Test
  void getRequestAttributes() {
    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertThrows(IllegalStateException.class, () -> grpcClientConnectionManager.getRequestAttributes(remoteChannel));

    final RequestAttributes requestAttributes = new RequestAttributes(InetAddresses.forString("6.7.8.9"), null, null);
    remoteChannel.attr(GrpcClientConnectionManager.REQUEST_ATTRIBUTES_KEY).set(requestAttributes);

    assertEquals(requestAttributes, grpcClientConnectionManager.getRequestAttributes(remoteChannel));
  }

  @Test
  void closeConnection() throws InterruptedException, ChannelNotFoundException {
    final AuthenticatedDevice authenticatedDevice = new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID);

    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.of(authenticatedDevice));

    assertTrue(remoteChannel.isOpen());

    assertEquals(remoteChannel, grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));
    assertEquals(List.of(remoteChannel),
        grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    remoteChannel.close().await();

    assertThrows(ChannelNotFoundException.class,
        () -> grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));

    assertNull(grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));
  }

  @ParameterizedTest
  @MethodSource
  void handleHandshakeInitiatedRequestAttributes(final InetAddress preferredRemoteAddress,
      final String userAgentHeader,
      final String acceptLanguageHeader,
      final RequestAttributes expectedRequestAttributes) {

    final EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    GrpcClientConnectionManager.handleHandshakeInitiated(embeddedChannel,
        preferredRemoteAddress,
        userAgentHeader,
        acceptLanguageHeader);

    assertEquals(expectedRequestAttributes,
        embeddedChannel.attr(GrpcClientConnectionManager.REQUEST_ATTRIBUTES_KEY).get());
  }

  private static List<Arguments> handleHandshakeInitiatedRequestAttributes() {
    final InetAddress preferredRemoteAddress = InetAddresses.forString("192.168.1.1");

    return List.of(
        Arguments.argumentSet("Null User-Agent and Accept-Language headers",
            preferredRemoteAddress, null, null,
            new RequestAttributes(preferredRemoteAddress, null, Collections.emptyList())),

        Arguments.argumentSet("Recognized User-Agent and null Accept-Language header",
            preferredRemoteAddress, "Signal-Desktop/1.2.3 Linux", null,
            new RequestAttributes(preferredRemoteAddress, "Signal-Desktop/1.2.3 Linux", Collections.emptyList())),

        Arguments.argumentSet("Unparsable User-Agent and null Accept-Language header",
            preferredRemoteAddress, "Not a valid user-agent string", null,
            new RequestAttributes(preferredRemoteAddress, "Not a valid user-agent string", Collections.emptyList())),

        Arguments.argumentSet("Null User-Agent and parsable Accept-Language header",
            preferredRemoteAddress, null, "ja,en;q=0.4",
            new RequestAttributes(preferredRemoteAddress, null, Locale.LanguageRange.parse("ja,en;q=0.4"))),

        Arguments.argumentSet("Null User-Agent and unparsable Accept-Language header",
            preferredRemoteAddress, null, "This is not a valid language preference list",
            new RequestAttributes(preferredRemoteAddress, null, Collections.emptyList()))
    );
  }

  @Test
  void handleConnectionEstablishedAuthenticated() throws InterruptedException, ChannelNotFoundException {
    final AuthenticatedDevice authenticatedDevice = new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID);

    assertThrows(ChannelNotFoundException.class,
        () -> grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));

    assertNull(grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.of(authenticatedDevice));

    assertEquals(remoteChannel, grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));
    assertEquals(List.of(remoteChannel), grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    remoteChannel.close().await();

    assertThrows(ChannelNotFoundException.class,
        () -> grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));

    assertNull(grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));
  }

  @Test
  void handleConnectionEstablishedAnonymous() throws InterruptedException, ChannelNotFoundException {
    assertThrows(ChannelNotFoundException.class,
        () -> grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));

    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(remoteChannel, grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));

    remoteChannel.close().await();

    assertThrows(ChannelNotFoundException.class,
        () -> grpcClientConnectionManager.getRemoteChannel(localChannel.localAddress()));
  }
}
