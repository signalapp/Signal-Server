package org.whispersystems.textsecuregcm.grpc.net.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.grpc.net.AbstractLeakDetectionTest;

class RejectUnsupportedMessagesHandlerTest extends AbstractLeakDetectionTest {

  private EmbeddedChannel embeddedChannel;

  @BeforeEach
  void setUp() {
    embeddedChannel = new EmbeddedChannel(new RejectUnsupportedMessagesHandler());
  }

  @ParameterizedTest
  @MethodSource
  void allowWebSocketFrame(final WebSocketFrame frame) {
    embeddedChannel.writeOneInbound(frame);

    try {
      assertEquals(frame, embeddedChannel.inboundMessages().poll());
      assertTrue(embeddedChannel.inboundMessages().isEmpty());
      assertEquals(1, frame.refCnt());
    } finally {
      frame.release();
    }
  }

  private static List<WebSocketFrame> allowWebSocketFrame() {
    return List.of(
        new BinaryWebSocketFrame(),
        new CloseWebSocketFrame(),
        new ContinuationWebSocketFrame(),
        new PingWebSocketFrame(),
        new PongWebSocketFrame());
  }

  @Test
  void rejectTextFrame() {
    final TextWebSocketFrame textFrame = new TextWebSocketFrame();
    embeddedChannel.writeOneInbound(textFrame);

    assertTrue(embeddedChannel.inboundMessages().isEmpty());
    assertEquals(0, textFrame.refCnt());
  }

  @Test
  void rejectNonWebSocketFrame() {
    final ByteBuf bytes = Unpooled.buffer(0);
    embeddedChannel.writeOneInbound(bytes);

    assertTrue(embeddedChannel.inboundMessages().isEmpty());
    assertEquals(0, bytes.refCnt());
  }
}
