package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.net.InetAddresses;
import com.vdurmont.semver4j.Semver;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;

class WebsocketHandshakeCompleteHandlerTest extends AbstractLeakDetectionTest {

  private UserEventRecordingHandler userEventRecordingHandler;
  private MutableRemoteAddressEmbeddedChannel embeddedChannel;

  private static final String RECOGNIZED_PROXY_SECRET = RandomStringUtils.randomAlphanumeric(16);

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
        new WebsocketHandshakeCompleteHandler(mock(ClientPublicKeysManager.class),
            Curve.generateKeyPair(),
            new byte[64],
            RECOGNIZED_PROXY_SECRET),
        userEventRecordingHandler);

    embeddedChannel.setRemoteAddress(new InetSocketAddress("127.0.0.1", 0));
  }

  @ParameterizedTest
  @MethodSource
  void handleWebSocketHandshakeComplete(final String uri, final Class<? extends AbstractNoiseHandshakeHandler> expectedHandlerClass) {
    final WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent =
        new WebSocketServerProtocolHandler.HandshakeComplete(uri, new DefaultHttpHeaders(), null);

    embeddedChannel.pipeline().fireUserEventTriggered(handshakeCompleteEvent);

    assertNull(embeddedChannel.pipeline().get(WebsocketHandshakeCompleteHandler.class));
    assertNotNull(embeddedChannel.pipeline().get(expectedHandlerClass));

    assertEquals(List.of(handshakeCompleteEvent), userEventRecordingHandler.getReceivedEvents());
  }

  private static List<Arguments> handleWebSocketHandshakeComplete() {
    return List.of(
        Arguments.of(WebsocketNoiseTunnelServer.AUTHENTICATED_SERVICE_PATH, NoiseXXHandshakeHandler.class),
        Arguments.of(WebsocketNoiseTunnelServer.ANONYMOUS_SERVICE_PATH, NoiseNXHandshakeHandler.class));
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
            WebsocketNoiseTunnelServer.ANONYMOUS_SERVICE_PATH, headers, null);

    embeddedChannel.setRemoteAddress(remoteAddress);
    embeddedChannel.pipeline().fireUserEventTriggered(handshakeCompleteEvent);

    assertEquals(expectedRemoteAddress,
        embeddedChannel.attr(ClientConnectionManager.REMOTE_ADDRESS_ATTRIBUTE_KEY).get());
  }

  private static List<Arguments> getRemoteAddress() {
    final InetSocketAddress remoteAddress = new InetSocketAddress("5.6.7.8", 0);
    final InetAddress clientAddress = InetAddresses.forString("1.2.3.4");
    final InetAddress proxyAddress = InetAddresses.forString("4.3.2.1");

    return List.of(
        // Recognized proxy, single forwarded-for address
        Arguments.of(new DefaultHttpHeaders()
            .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
            .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress()),
            remoteAddress,
            clientAddress),

        // Recognized proxy, multiple forwarded-for addresses
        Arguments.of(new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress() + "," + proxyAddress.getHostAddress()),
            remoteAddress,
            proxyAddress),

        // No recognized proxy header, single forwarded-for address
        Arguments.of(new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress()),
            remoteAddress,
            remoteAddress.getAddress()),

        // No recognized proxy header, no forwarded-for address
        Arguments.of(new DefaultHttpHeaders(),
            remoteAddress,
            remoteAddress.getAddress()),

        // Incorrect proxy header, single forwarded-for address
        Arguments.of(new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET + "-incorrect")
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, clientAddress.getHostAddress()),
            remoteAddress,
            remoteAddress.getAddress()),

        // Recognized proxy, no forwarded-for address
        Arguments.of(new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET),
            remoteAddress,
            remoteAddress.getAddress()),

        // Recognized proxy, bogus forwarded-for address
        Arguments.of(new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET)
                .add(WebsocketHandshakeCompleteHandler.FORWARDED_FOR_HEADER, "not a valid address"),
            remoteAddress,
            null),

        // No forwarded-for address, non-InetSocketAddress remote address
        Arguments.of(new DefaultHttpHeaders()
                .add(WebsocketHandshakeCompleteHandler.RECOGNIZED_PROXY_SECRET_HEADER, RECOGNIZED_PROXY_SECRET),
            new LocalAddress("local-address"),
            null)
    );
  }
}
