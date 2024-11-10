package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.net.InetAddresses;
import com.vdurmont.semver4j.Semver;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

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
        grpcClientConnectionManager.getAuthenticatedDevice(localChannel.localAddress()));
  }

  private static List<Optional<AuthenticatedDevice>> getAuthenticatedDevice() {
    return List.of(
        Optional.of(new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID)),
        Optional.empty()
    );
  }

  @Test
  void getAcceptableLanguages() {
    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(Optional.empty(),
        grpcClientConnectionManager.getAcceptableLanguages(localChannel.localAddress()));

    final List<Locale.LanguageRange> acceptLanguageRanges = Locale.LanguageRange.parse("en,ja");
    remoteChannel.attr(GrpcClientConnectionManager.ACCEPT_LANGUAGE_ATTRIBUTE_KEY).set(acceptLanguageRanges);

    assertEquals(Optional.of(acceptLanguageRanges),
        grpcClientConnectionManager.getAcceptableLanguages(localChannel.localAddress()));
  }

  @Test
  void getRemoteAddress() {
    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(Optional.empty(),
        grpcClientConnectionManager.getRemoteAddress(localChannel.localAddress()));

    final InetAddress remoteAddress = InetAddresses.forString("6.7.8.9");
    remoteChannel.attr(GrpcClientConnectionManager.REMOTE_ADDRESS_ATTRIBUTE_KEY).set(remoteAddress);

    assertEquals(Optional.of(remoteAddress),
        grpcClientConnectionManager.getRemoteAddress(localChannel.localAddress()));
  }

  @Test
  void getUserAgent() throws UnrecognizedUserAgentException {
    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(Optional.empty(),
        grpcClientConnectionManager.getUserAgent(localChannel.localAddress()));

    final UserAgent userAgent = UserAgentUtil.parseUserAgentString("Signal-Desktop/1.2.3 Linux");
    remoteChannel.attr(GrpcClientConnectionManager.PARSED_USER_AGENT_ATTRIBUTE_KEY).set(userAgent);

    assertEquals(Optional.of(userAgent),
        grpcClientConnectionManager.getUserAgent(localChannel.localAddress()));
  }

  @Test
  void closeConnection() throws InterruptedException {
    final AuthenticatedDevice authenticatedDevice = new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID);

    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.of(authenticatedDevice));

    assertTrue(remoteChannel.isOpen());

    assertEquals(remoteChannel, grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertEquals(List.of(remoteChannel),
        grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    remoteChannel.close().await();

    assertNull(grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertNull(grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));
  }

  @Test
  void handleWebSocketHandshakeCompleteRemoteAddress() {
    final EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    final InetAddress preferredRemoteAddress = InetAddresses.forString("192.168.1.1");

    GrpcClientConnectionManager.handleWebSocketHandshakeComplete(embeddedChannel,
        preferredRemoteAddress,
        null,
        null);

    assertEquals(preferredRemoteAddress,
        embeddedChannel.attr(GrpcClientConnectionManager.REMOTE_ADDRESS_ATTRIBUTE_KEY).get());
  }

  @ParameterizedTest
  @MethodSource
  void handleWebSocketHandshakeCompleteUserAgent(@Nullable final String userAgentHeader,
      @Nullable final UserAgent expectedParsedUserAgent) {

    final EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    GrpcClientConnectionManager.handleWebSocketHandshakeComplete(embeddedChannel,
        InetAddresses.forString("127.0.0.1"),
        userAgentHeader,
        null);

    assertEquals(userAgentHeader,
        embeddedChannel.attr(GrpcClientConnectionManager.RAW_USER_AGENT_ATTRIBUTE_KEY).get());

    assertEquals(expectedParsedUserAgent,
        embeddedChannel.attr(GrpcClientConnectionManager.PARSED_USER_AGENT_ATTRIBUTE_KEY).get());
  }

  private static List<Arguments> handleWebSocketHandshakeCompleteUserAgent() {
    return List.of(
        // Recognized user-agent
        Arguments.of("Signal-Desktop/1.2.3 Linux", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Linux")),

        // Unrecognized user-agent
        Arguments.of("Not a valid user-agent string", null),

        // Missing user-agent
        Arguments.of(null, null)
    );
  }


  @ParameterizedTest
  @MethodSource
  void handleWebSocketHandshakeCompleteAcceptLanguage(@Nullable final String acceptLanguageHeader,
      @Nullable final List<Locale.LanguageRange> expectedLanguageRanges) {

    final EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    GrpcClientConnectionManager.handleWebSocketHandshakeComplete(embeddedChannel,
        InetAddresses.forString("127.0.0.1"),
        null,
        acceptLanguageHeader);

    assertEquals(expectedLanguageRanges,
        embeddedChannel.attr(GrpcClientConnectionManager.ACCEPT_LANGUAGE_ATTRIBUTE_KEY).get());
  }

  private static List<Arguments> handleWebSocketHandshakeCompleteAcceptLanguage() {
    return List.of(
        // Parseable list
        Arguments.of("ja,en;q=0.4", Locale.LanguageRange.parse("ja,en;q=0.4")),

        // Unparsable list
        Arguments.of("This is not a valid language preference list", null),

        // Missing list
        Arguments.of(null, null)
    );
  }

  @Test
  void handleConnectionEstablishedAuthenticated() throws InterruptedException {
    final AuthenticatedDevice authenticatedDevice = new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID);

    assertNull(grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertNull(grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.of(authenticatedDevice));

    assertEquals(remoteChannel, grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertEquals(List.of(remoteChannel), grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    remoteChannel.close().await();

    assertNull(grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertNull(grpcClientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));
  }

  @Test
  void handleConnectionEstablishedAnonymous() throws InterruptedException {
    assertNull(grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));

    grpcClientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(remoteChannel, grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));

    remoteChannel.close().await();

    assertNull(grpcClientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
  }
}
