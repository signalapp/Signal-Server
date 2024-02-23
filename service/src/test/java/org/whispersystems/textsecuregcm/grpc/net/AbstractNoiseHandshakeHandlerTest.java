package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

abstract class AbstractNoiseHandshakeHandlerTest extends AbstractLeakDetectionTest {

  private ECPublicKey rootPublicKey;

  private NoiseHandshakeCompleteHandler noiseHandshakeCompleteHandler;

  private EmbeddedChannel embeddedChannel;

  private static class NoiseHandshakeCompleteHandler extends ChannelInboundHandlerAdapter {

    @Nullable
    private NoiseHandshakeCompleteEvent handshakeCompleteEvent = null;

    @Override
    public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
      if (event instanceof NoiseHandshakeCompleteEvent noiseHandshakeCompleteEvent) {
        handshakeCompleteEvent = noiseHandshakeCompleteEvent;
      } else {
        context.fireUserEventTriggered(event);
      }
    }

    @Nullable
    public NoiseHandshakeCompleteEvent getHandshakeCompleteEvent() {
      return handshakeCompleteEvent;
    }
  }

  @BeforeEach
  void setUp() {
    final ECKeyPair rootKeyPair = Curve.generateKeyPair();
    final ECKeyPair serverKeyPair = Curve.generateKeyPair();

    rootPublicKey = rootKeyPair.getPublicKey();

    final byte[] serverPublicKeySignature =
        rootKeyPair.getPrivateKey().calculateSignature(serverKeyPair.getPublicKey().getPublicKeyBytes());

    noiseHandshakeCompleteHandler = new NoiseHandshakeCompleteHandler();

    embeddedChannel =
        new EmbeddedChannel(getHandler(serverKeyPair, serverPublicKeySignature), noiseHandshakeCompleteHandler);
  }

  @AfterEach
  void tearDown() {
    embeddedChannel.close();
  }

  protected EmbeddedChannel getEmbeddedChannel() {
    return embeddedChannel;
  }

  protected ECPublicKey getRootPublicKey() {
    return rootPublicKey;
  }

  @Nullable
  protected NoiseHandshakeCompleteEvent getNoiseHandshakeCompleteEvent() {
    return noiseHandshakeCompleteHandler.getHandshakeCompleteEvent();
  }

  protected abstract AbstractNoiseHandshakeHandler getHandler(final ECKeyPair serverKeyPair, final byte[] serverPublicKeySignature);

  @Test
  void handleInvalidInitialMessage() throws InterruptedException {
    final byte[] contentBytes = new byte[17];
    ThreadLocalRandom.current().nextBytes(contentBytes);

    final ByteBuf content = Unpooled.wrappedBuffer(contentBytes);

    final ChannelFuture writeFuture = embeddedChannel.writeOneInbound(new BinaryWebSocketFrame(content)).await();

    assertFalse(writeFuture.isSuccess());
    assertInstanceOf(NoiseHandshakeException.class, writeFuture.cause());
    assertEquals(0, content.refCnt());
    assertNull(getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleMessagesAfterInitialHandshakeFailure() throws InterruptedException {
    final BinaryWebSocketFrame[] frames = new BinaryWebSocketFrame[7];

    for (int i = 0; i < frames.length; i++) {
      final byte[] contentBytes = new byte[17];
      ThreadLocalRandom.current().nextBytes(contentBytes);

      frames[i] = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(contentBytes));

      embeddedChannel.writeOneInbound(frames[i]).await();
    }

    for (final BinaryWebSocketFrame frame : frames) {
      assertEquals(0, frame.refCnt());
    }

    assertNull(getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleNonWebSocketBinaryFrame() throws InterruptedException {
    final byte[] contentBytes = new byte[17];
    ThreadLocalRandom.current().nextBytes(contentBytes);

    final ByteBuf message = Unpooled.wrappedBuffer(contentBytes);

    final ChannelFuture writeFuture = embeddedChannel.writeOneInbound(message).await();

    assertFalse(writeFuture.isSuccess());
    assertInstanceOf(IllegalArgumentException.class, writeFuture.cause());
    assertEquals(0, message.refCnt());
    assertNull(getNoiseHandshakeCompleteEvent());

    assertTrue(embeddedChannel.inboundMessages().isEmpty());
  }
}
