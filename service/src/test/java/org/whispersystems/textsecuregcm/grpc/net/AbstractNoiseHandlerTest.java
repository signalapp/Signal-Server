package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.southernstorm.noise.protocol.CipherStatePair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

abstract class AbstractNoiseHandlerTest extends AbstractLeakDetectionTest {

  protected ECKeyPair serverKeyPair;

  private NoiseHandshakeCompleteHandler noiseHandshakeCompleteHandler;

  private EmbeddedChannel embeddedChannel;

  private static class PongHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
      try {
        if (msg instanceof ByteBuf bb) {
          if (new String(ByteBufUtil.getBytes(bb)).equals("ping")) {
            ctx.writeAndFlush(Unpooled.wrappedBuffer("pong".getBytes()))
                .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
          } else {
            throw new IllegalArgumentException("Unexpected message: " + new String(ByteBufUtil.getBytes(bb)));
          }
        } else {
          throw new IllegalArgumentException("Unexpected message type: " + msg);
        }
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }
  }

  private static class NoiseHandshakeCompleteHandler extends ChannelInboundHandlerAdapter {

    @Nullable
    private NoiseIdentityDeterminedEvent handshakeCompleteEvent = null;

    @Override
    public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
      if (event instanceof NoiseIdentityDeterminedEvent noiseIdentityDeterminedEvent) {
        handshakeCompleteEvent = noiseIdentityDeterminedEvent;
        context.pipeline().addAfter(context.name(), null, new PongHandler());
        context.pipeline().remove(NoiseHandshakeCompleteHandler.class);
      } else {
        context.fireUserEventTriggered(event);
      }
    }

    @Nullable
    public NoiseIdentityDeterminedEvent getHandshakeCompleteEvent() {
      return handshakeCompleteEvent;
    }
  }

  @BeforeEach
  void setUp() {
    serverKeyPair = Curve.generateKeyPair();
    noiseHandshakeCompleteHandler = new NoiseHandshakeCompleteHandler();
    embeddedChannel = new EmbeddedChannel(getHandler(serverKeyPair), noiseHandshakeCompleteHandler);
  }

  @AfterEach
  void tearDown() {
    embeddedChannel.close();
  }

  protected EmbeddedChannel getEmbeddedChannel() {
    return embeddedChannel;
  }

  @Nullable
  protected NoiseIdentityDeterminedEvent getNoiseHandshakeCompleteEvent() {
    return noiseHandshakeCompleteHandler.getHandshakeCompleteEvent();
  }

  protected abstract ChannelHandler getHandler(final ECKeyPair serverKeyPair);

  protected abstract CipherStatePair doHandshake() throws Throwable;

  /**
   * Read a message from the embedded channel and deserialize it with the provided client cipher state. If there are no
   * waiting messages in the channel, return null.
   */
  byte[] readNextPlaintext(final CipherStatePair clientCipherPair) throws ShortBufferException, BadPaddingException {
    final BinaryWebSocketFrame responseFrame = (BinaryWebSocketFrame) embeddedChannel.outboundMessages().poll();
    if (responseFrame == null) {
      return null;
    }
    final byte[] plaintext = new byte[responseFrame.content().readableBytes() - 16];
    final int read = clientCipherPair.getReceiver().decryptWithAd(null,
        ByteBufUtil.getBytes(responseFrame.content()), 0,
        plaintext, 0,
        responseFrame.content().readableBytes());
    assertEquals(read, plaintext.length);
    return plaintext;
  }


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
  void handleNonWebSocketBinaryFrame() throws Throwable {
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

  @Test
  void channelRead() throws Throwable {
    final CipherStatePair clientCipherStatePair = doHandshake();
    final byte[] plaintext = "ping".getBytes(StandardCharsets.UTF_8);
    final byte[] ciphertext = new byte[plaintext.length + clientCipherStatePair.getSender().getMACLength()];
    clientCipherStatePair.getSender().encryptWithAd(null, plaintext, 0, ciphertext, 0, plaintext.length);

    final BinaryWebSocketFrame ciphertextFrame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(ciphertext));
    assertTrue(embeddedChannel.writeOneInbound(ciphertextFrame).await().isSuccess());
    assertEquals(0, ciphertextFrame.refCnt());

    final byte[] response = readNextPlaintext(clientCipherStatePair);
    assertArrayEquals("pong".getBytes(StandardCharsets.UTF_8), response);
  }

  @Test
  void channelReadBadCiphertext() throws Throwable {
    doHandshake();
    final byte[] bogusCiphertext = new byte[32];
    io.netty.util.internal.ThreadLocalRandom.current().nextBytes(bogusCiphertext);

    final BinaryWebSocketFrame ciphertextFrame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bogusCiphertext));
    final ChannelFuture readCiphertextFuture = embeddedChannel.writeOneInbound(ciphertextFrame).await();

    assertEquals(0, ciphertextFrame.refCnt());
    assertFalse(readCiphertextFuture.isSuccess());
    assertInstanceOf(AEADBadTagException.class, readCiphertextFuture.cause());
    assertTrue(embeddedChannel.inboundMessages().isEmpty());
  }

  @Test
  void channelReadUnexpectedMessageType() throws Throwable {
    doHandshake();
    final ChannelFuture readFuture = embeddedChannel.writeOneInbound(new Object()).await();

    assertFalse(readFuture.isSuccess());
    assertInstanceOf(IllegalArgumentException.class, readFuture.cause());
    assertTrue(embeddedChannel.inboundMessages().isEmpty());
  }

  @Test
  void write() throws Throwable {
    final CipherStatePair clientCipherStatePair = doHandshake();
    final byte[] plaintext = "A plaintext message".getBytes(StandardCharsets.UTF_8);
    final ByteBuf plaintextBuffer = Unpooled.wrappedBuffer(plaintext);

    final ChannelFuture writePlaintextFuture = embeddedChannel.pipeline().writeAndFlush(plaintextBuffer);
    assertTrue(writePlaintextFuture.await().isSuccess());
    assertEquals(0, plaintextBuffer.refCnt());

    final BinaryWebSocketFrame ciphertextFrame = (BinaryWebSocketFrame) embeddedChannel.outboundMessages().poll();
    assertNotNull(ciphertextFrame);
    assertTrue(embeddedChannel.outboundMessages().isEmpty());

    final byte[] ciphertext = ByteBufUtil.getBytes(ciphertextFrame.content());
    ciphertextFrame.release();

    final byte[] decryptedPlaintext = new byte[ciphertext.length - clientCipherStatePair.getReceiver().getMACLength()];
    clientCipherStatePair.getReceiver().decryptWithAd(null, ciphertext, 0, decryptedPlaintext, 0, ciphertext.length);

    assertArrayEquals(plaintext, decryptedPlaintext);
  }

  @Test
  void writeUnexpectedMessageType() throws Throwable {
    doHandshake();
    final Object unexpectedMessaged = new Object();

    final ChannelFuture writeFuture = embeddedChannel.pipeline().writeAndFlush(unexpectedMessaged);
    assertTrue(writeFuture.await().isSuccess());

    assertEquals(unexpectedMessaged, embeddedChannel.outboundMessages().poll());
    assertTrue(embeddedChannel.outboundMessages().isEmpty());
  }

}
