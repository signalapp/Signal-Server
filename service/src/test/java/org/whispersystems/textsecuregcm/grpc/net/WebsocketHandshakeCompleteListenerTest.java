package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class WebsocketHandshakeCompleteListenerTest extends AbstractLeakDetectionTest {

  private UserEventRecordingHandler userEventRecordingHandler;
  private EmbeddedChannel embeddedChannel;

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

  @BeforeEach
  void setUp() {
    userEventRecordingHandler = new UserEventRecordingHandler();

    embeddedChannel = new EmbeddedChannel(
        new WebsocketHandshakeCompleteListener(mock(ClientPublicKeysManager.class), Curve.generateKeyPair(), new byte[64]),
        userEventRecordingHandler);
  }

  @ParameterizedTest
  @MethodSource
  void handleWebSocketHandshakeComplete(final String uri, final Class<? extends AbstractNoiseHandshakeHandler> expectedHandlerClass) {
    final WebSocketServerProtocolHandler.HandshakeComplete handshakeCompleteEvent =
        new WebSocketServerProtocolHandler.HandshakeComplete(uri, new DefaultHttpHeaders(), null);

    embeddedChannel.pipeline().fireUserEventTriggered(handshakeCompleteEvent);

    assertNull(embeddedChannel.pipeline().get(WebsocketHandshakeCompleteListener.class));
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

    assertNotNull(embeddedChannel.pipeline().get(WebsocketHandshakeCompleteListener.class));
    assertThrows(IllegalArgumentException.class, () -> embeddedChannel.checkException());
  }

  @Test
  void handleUnrecognizedEvent() {
    final Object unrecognizedEvent = new Object();

    embeddedChannel.pipeline().fireUserEventTriggered(unrecognizedEvent);
    assertEquals(List.of(unrecognizedEvent), userEventRecordingHandler.getReceivedEvents());
  }
}
