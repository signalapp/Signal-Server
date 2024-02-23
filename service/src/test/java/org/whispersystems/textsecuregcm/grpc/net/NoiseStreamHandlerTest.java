package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.CipherStatePair;
import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;

class NoiseStreamHandlerTest extends AbstractLeakDetectionTest {

  private CipherStatePair clientCipherStatePair;
  private EmbeddedChannel embeddedChannel;

  // We use an NN handshake for this test just because it's a little shorter and easier to set up
  private static final String NOISE_PROTOCOL_NAME = "Noise_NN_25519_ChaChaPoly_BLAKE2b";

  @BeforeEach
  void setUp() throws NoSuchAlgorithmException, ShortBufferException, BadPaddingException {
    final HandshakeState clientHandshakeState = new HandshakeState(NOISE_PROTOCOL_NAME, HandshakeState.INITIATOR);
    final HandshakeState serverHandshakeState = new HandshakeState(NOISE_PROTOCOL_NAME, HandshakeState.RESPONDER);

    clientHandshakeState.start();
    serverHandshakeState.start();

    final byte[] clientEphemeralKeyMessage = new byte[32];
    assertEquals(clientEphemeralKeyMessage.length,
        clientHandshakeState.writeMessage(clientEphemeralKeyMessage, 0, null, 0, 0));

    serverHandshakeState.readMessage(clientEphemeralKeyMessage, 0, clientEphemeralKeyMessage.length, EmptyArrays.EMPTY_BYTES, 0);

    // 32 bytes of key material plus a 16-byte MAC
    final byte[] serverEphemeralKeyMessage = new byte[48];
    assertEquals(serverEphemeralKeyMessage.length,
        serverHandshakeState.writeMessage(serverEphemeralKeyMessage, 0, null, 0, 0));

    clientHandshakeState.readMessage(serverEphemeralKeyMessage, 0, serverEphemeralKeyMessage.length, EmptyArrays.EMPTY_BYTES, 0);

    clientCipherStatePair = clientHandshakeState.split();
    embeddedChannel = new EmbeddedChannel(new NoiseStreamHandler(serverHandshakeState.split()));

    clientHandshakeState.destroy();
    serverHandshakeState.destroy();
  }

  @Test
  void channelRead() throws ShortBufferException, InterruptedException {
    final byte[] plaintext = "A plaintext message".getBytes(StandardCharsets.UTF_8);
    final byte[] ciphertext = new byte[plaintext.length + clientCipherStatePair.getSender().getMACLength()];
    clientCipherStatePair.getSender().encryptWithAd(null, plaintext, 0, ciphertext, 0, plaintext.length);

    final BinaryWebSocketFrame ciphertextFrame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(ciphertext));
    assertTrue(embeddedChannel.writeOneInbound(ciphertextFrame).await().isSuccess());
    assertEquals(0, ciphertextFrame.refCnt());

    final ByteBuf decryptedPlaintextBuffer = (ByteBuf) embeddedChannel.inboundMessages().poll();
    assertNotNull(decryptedPlaintextBuffer);
    assertTrue(embeddedChannel.inboundMessages().isEmpty());

    final byte[] decryptedPlaintext = ByteBufUtil.getBytes(decryptedPlaintextBuffer);
    decryptedPlaintextBuffer.release();

    assertArrayEquals(plaintext, decryptedPlaintext);
  }

  @Test
  void channelReadBadCiphertext() throws InterruptedException {
    final byte[] bogusCiphertext = new byte[32];
    ThreadLocalRandom.current().nextBytes(bogusCiphertext);

    final BinaryWebSocketFrame ciphertextFrame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bogusCiphertext));
    final ChannelFuture readCiphertextFuture = embeddedChannel.writeOneInbound(ciphertextFrame).await();

    assertEquals(0, ciphertextFrame.refCnt());
    assertFalse(readCiphertextFuture.isSuccess());
    assertInstanceOf(AEADBadTagException.class, readCiphertextFuture.cause());
    assertTrue(embeddedChannel.inboundMessages().isEmpty());
  }

  @Test
  void channelReadUnexpectedMessageType() throws InterruptedException {
    final ChannelFuture readFuture = embeddedChannel.writeOneInbound(new Object()).await();

    assertFalse(readFuture.isSuccess());
    assertInstanceOf(IllegalArgumentException.class, readFuture.cause());
    assertTrue(embeddedChannel.inboundMessages().isEmpty());
  }

  @Test
  void write() throws InterruptedException, ShortBufferException, BadPaddingException {
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
  void writeUnexpectedMessageType() throws InterruptedException {
    final Object unexpectedMessaged = new Object();

    final ChannelFuture writeFuture = embeddedChannel.pipeline().writeAndFlush(unexpectedMessaged);
    assertTrue(writeFuture.await().isSuccess());

    assertEquals(unexpectedMessaged, embeddedChannel.outboundMessages().poll());
    assertTrue(embeddedChannel.outboundMessages().isEmpty());
  }
}
