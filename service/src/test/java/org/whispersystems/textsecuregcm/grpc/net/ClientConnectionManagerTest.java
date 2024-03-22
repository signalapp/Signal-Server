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

class ClientConnectionManagerTest {

  private static EventLoopGroup eventLoopGroup;

  private LocalChannel localChannel;
  private LocalChannel remoteChannel;

  private LocalServerChannel localServerChannel;

  private ClientConnectionManager clientConnectionManager;

  @BeforeAll
  static void setUpBeforeAll() {
    eventLoopGroup = new DefaultEventLoopGroup();
  }

  @BeforeEach
  void setUp() throws InterruptedException {
    eventLoopGroup = new DefaultEventLoopGroup();

    clientConnectionManager = new ClientConnectionManager();

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
    clientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, maybeAuthenticatedDevice);

    assertEquals(maybeAuthenticatedDevice,
        clientConnectionManager.getAuthenticatedDevice(localChannel.localAddress()));
  }

  private static List<Optional<AuthenticatedDevice>> getAuthenticatedDevice() {
    return List.of(
        Optional.of(new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID)),
        Optional.empty()
    );
  }

  @Test
  void getAcceptableLanguages() {
    clientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(Optional.empty(),
        clientConnectionManager.getAcceptableLanguages(localChannel.localAddress()));

    final List<Locale.LanguageRange> acceptLanguageRanges = Locale.LanguageRange.parse("en,ja");
    remoteChannel.attr(ClientConnectionManager.ACCEPT_LANGUAGE_ATTRIBUTE_KEY).set(acceptLanguageRanges);

    assertEquals(Optional.of(acceptLanguageRanges),
        clientConnectionManager.getAcceptableLanguages(localChannel.localAddress()));
  }

  @Test
  void getRemoteAddress() {
    clientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(Optional.empty(),
        clientConnectionManager.getRemoteAddress(localChannel.localAddress()));

    final InetAddress remoteAddress = InetAddresses.forString("6.7.8.9");
    remoteChannel.attr(ClientConnectionManager.REMOTE_ADDRESS_ATTRIBUTE_KEY).set(remoteAddress);

    assertEquals(Optional.of(remoteAddress),
        clientConnectionManager.getRemoteAddress(localChannel.localAddress()));
  }

  @Test
  void getUserAgent() throws UnrecognizedUserAgentException {
    clientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(Optional.empty(),
        clientConnectionManager.getUserAgent(localChannel.localAddress()));

    final UserAgent userAgent = UserAgentUtil.parseUserAgentString("Signal-Desktop/1.2.3 Linux");
    remoteChannel.attr(ClientConnectionManager.PARSED_USER_AGENT_ATTRIBUTE_KEY).set(userAgent);

    assertEquals(Optional.of(userAgent),
        clientConnectionManager.getUserAgent(localChannel.localAddress()));
  }

  @Test
  void closeConnection() throws InterruptedException {
    final AuthenticatedDevice authenticatedDevice = new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID);

    clientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.of(authenticatedDevice));

    assertTrue(remoteChannel.isOpen());

    assertEquals(remoteChannel, clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertEquals(List.of(remoteChannel),
        clientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    remoteChannel.close().await();

    assertNull(clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertNull(clientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));
  }

  @Test
  void handleWebSocketHandshakeCompleteRemoteAddress() {
    final EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    final InetAddress preferredRemoteAddress = InetAddresses.forString("192.168.1.1");

    ClientConnectionManager.handleWebSocketHandshakeComplete(embeddedChannel,
        preferredRemoteAddress,
        null,
        null);

    assertEquals(preferredRemoteAddress,
        embeddedChannel.attr(ClientConnectionManager.REMOTE_ADDRESS_ATTRIBUTE_KEY).get());
  }

  @ParameterizedTest
  @MethodSource
  void handleWebSocketHandshakeCompleteUserAgent(@Nullable final String userAgentHeader,
      @Nullable final UserAgent expectedParsedUserAgent) {

    final EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    ClientConnectionManager.handleWebSocketHandshakeComplete(embeddedChannel,
        InetAddresses.forString("127.0.0.1"),
        userAgentHeader,
        null);

    assertEquals(userAgentHeader,
        embeddedChannel.attr(ClientConnectionManager.RAW_USER_AGENT_ATTRIBUTE_KEY).get());

    assertEquals(expectedParsedUserAgent,
        embeddedChannel.attr(ClientConnectionManager.PARSED_USER_AGENT_ATTRIBUTE_KEY).get());
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

    ClientConnectionManager.handleWebSocketHandshakeComplete(embeddedChannel,
        InetAddresses.forString("127.0.0.1"),
        null,
        acceptLanguageHeader);

    assertEquals(expectedLanguageRanges,
        embeddedChannel.attr(ClientConnectionManager.ACCEPT_LANGUAGE_ATTRIBUTE_KEY).get());
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

    assertNull(clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertNull(clientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    clientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.of(authenticatedDevice));

    assertEquals(remoteChannel, clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertEquals(List.of(remoteChannel), clientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));

    remoteChannel.close().await();

    assertNull(clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
    assertNull(clientConnectionManager.getRemoteChannelsByAuthenticatedDevice(authenticatedDevice));
  }

  @Test
  void handleConnectionEstablishedAnonymous() throws InterruptedException {
    assertNull(clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));

    clientConnectionManager.handleConnectionEstablished(localChannel, remoteChannel, Optional.empty());

    assertEquals(remoteChannel, clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));

    remoteChannel.close().await();

    assertNull(clientConnectionManager.getRemoteChannelByLocalAddress(localChannel.localAddress()));
  }
}
