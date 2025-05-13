package org.whispersystems.textsecuregcm.grpc.net.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.net.InetAddresses;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.grpc.net.AbstractLeakDetectionTest;
import org.whispersystems.textsecuregcm.grpc.net.HandshakePattern;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeInit;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class WebsocketHandshakeCompleteHandlerTest extends AbstractLeakDetectionTest {

  private UserEventRecordingHandler userEventRecordingHandler;
  private MutableRemoteAddressEmbeddedChannel embeddedChannel;

  private static final String RECOGNIZED_PROXY_SECRET = RandomStringUtils.secure().nextAlphanumeric(16);

  private static class UserEventRecordingHandler extends ChannelInboundHandlerAdapter {

    private final List<Object> receivedEvents = new ArrayList<>();

    @Override
    public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
      receivedEvents.add(event);
    }

    public List<Object> getReceivedEvents() {
      return receivedEvents;
    }
  }

  private static class MutableRemoteAddressEmbeddedChannel extends EmbeddedChannel {

    private SocketAddress remoteAddress;

    public MutableRemoteAddressEmbeddedChannel(final ChannelHandler... handlers) {
      super(handlers);
    }

    @Override
    protected SocketAddress remoteAddress0() {
      return isActive() ? remoteAddress : null;
    }

    public void setRemoteAddress(final SocketAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }
  }

  @BeforeEach
  void setUp() {
    userEventRecordingHandler = new UserEventRecordingHandler();

    embeddedChannel = new MutableRemoteAddressEmbeddedChannel(
        new WebsocketHandshakeCompleteHandler(RECOGNIZED_PROXY_SECRET),
        userEventRecordingHandler);

    embeddedChannel.setRemoteAddress(new InetSocketAddress("127.0.0.1", 0));
  }

  @ParameterizedTest
  @MethodSource
  void handleWebSocketHandshakeComplete(final String uri, final HandshakePattern pattern) {
    final WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent =
        new WebSocketServerProtocolHandler.HandshakeComplete(uri, new DefaultHttpHeaders(), null);

    embeddedChannel.pipeline().fireUserEventTriggered(handshakeCompleteEvent);
    assertEquals(List.of(handshakeCompleteEvent), userEventRecordingHandler.getReceivedEvents());

    final byte[] payload = TestRandomUtil.nextBytes(100);
    embeddedChannel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(payload));
    assertNull(embeddedChannel.pipeline().get(WebsocketHandshakeCompleteHandler.class));
    final NoiseHandshakeInit init = (NoiseHandshakeInit) embeddedChannel.inboundMessages().poll();
    assertNotNull(init);
    assertEquals(init.getHandshakePattern(), pattern);
  }

  private static List<Arguments> handleWebSocketHandshakeComplete() {
    return List.of(
        Arguments.of(NoiseWebSocketTunnelServer.AUTHENTICATED_SERVICE_PATH, HandshakePattern.IK),
        Arguments.of(NoiseWebSocketTunnelServer.ANONYMOUS_SERVICE_PATH, HandshakePattern.NK));
  }

  @Test
  void handleWebSocketHandshakeCompleteUnexpectedPath() {
    final WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent =
        new WebSocketServerProtocolHandler.HandshakeComplete("/incorrect", new DefaultHttpHeaders(), null);

    embeddedChannel.pipeline().fireUserEventTriggered(handshakeCompleteEvent);

    assertNotNull(embeddedChannel.pipeline().get(WebsocketHandshakeCompleteHandler.class));
    assertThrows(IllegalArgumentException.class, () -> embeddedChannel.checkException());
  }

  @Test
  void handleUnrecognizedEvent() {
    final Object unrecognizedEvent = new Object();

    embeddedChannel.pipeline().fireUserEventTriggered(unrecognizedEvent);
    assertEquals(List.of(unrecognizedEvent), userEventRecordingHandler.getReceivedEvents());
  }

  @ParameterizedTest
  @MethodSource
  void getRemoteAddress(final HttpHeaders headers, final SocketAddress remoteAddress, @Nullable InetAddress expectedRemoteAddress) {
    final WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent =
        new WebSocketServerProtocolHandler.HandshakeComplete(
            NoiseWebSocketTunnelServer.ANONYMOUS_SERVICE_PATH, headers, null);

    embeddedChannel.setRemoteAddress(remoteAddress);
    embeddedChannel.pipeline().fireUserEventTriggered(handshakeCompleteEvent);

    final byte[] payload = TestRandomUtil.nextBytes(100);
    embeddedChannel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(payload));
    final NoiseHandshakeInit init = (NoiseHandshakeInit) embeddedChannel.inboundMessages().poll();
    assertEquals(
        expectedRemoteAddress,
        Optional.ofNullable(init)
            .map(NoiseHandshakeInit::getRemoteAddress)
            .orElse(null));
    if (expectedRemoteAddress == null) {
      assertThrows(IllegalStateException.class, embeddedChannel::checkException);
    } else {
      assertNull(embeddedChannel.pipeline().get(WebsocketHandshakeCompleteHandler.class));
    }
  }

  private static List<Arguments> getRemoteAddress() {
    final InetSocketAddress remoteAddress = new InetSocketAddress("5.6.7.8", 0);
    final InetAddress clientAddress = InetAddresses.forString("1.2.3.4");
    final InetAddress proxyAddress = InetAddresses.forString("4.3.2.1");

    return List.of(
        argumentSet("Recognized proxy, single forwarded-for address",
            new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress()),
            remoteAddress,
            clientAddress),

        argumentSet("Recognized proxy, multiple forwarded-for addresses",
            new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress() + "," + proxyAddress.getHostAddress()),
            remoteAddress,
            proxyAddress),

        argumentSet("No recognized proxy header, single forwarded-for address",
            new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress()),
            remoteAddress,
            remoteAddress.getAddress()),

        argumentSet("No recognized proxy header, no forwarded-for address",
            new DefaultHttpHeaders(),
            remoteAddress,
            remoteAddress.getAddress()),

        argumentSet("Incorrect proxy header, single forwarded-for address",
            new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET + "-incorrect")
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress()),
            remoteAddress,
            remoteAddress.getAddress()),

        argumentSet("Recognized proxy, no forwarded-for address",
            new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET),
            remoteAddress,
            remoteAddress.getAddress()),

        argumentSet("Recognized proxy, bogus forwarded-for address",
            new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, "not a valid address"),
            remoteAddress,
            null),

        argumentSet("No forwarded-for address, non-InetSocketAddress remote address",
            new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET),
            new LocalAddress("local-address"),
            null)
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource("argumentsForGetMostRecentProxy")
  void getMostRecentProxy(final String forwardedFor, final Optional<String> expectedMostRecentProxy) {
    assertEquals(expectedMostRecentProxy, WebsocketHandshakeCompleteHandler.getMostRecentProxy(forwardedFor));
  }

  private static Stream<Arguments> argumentsForGetMostRecentProxy() {
    return Stream.of(
        arguments(null, Optional.empty()),
        arguments("", Optional.empty()),
        arguments("    ", Optional.empty()),
        arguments("203.0.113.195,", Optional.empty()),
        arguments("203.0.113.195, ", Optional.empty()),
        arguments("203.0.113.195", Optional.of("203.0.113.195")),
        arguments("203.0.113.195, 70.41.3.18, 150.172.238.178", Optional.of("150.172.238.178"))
    );
  }
}
