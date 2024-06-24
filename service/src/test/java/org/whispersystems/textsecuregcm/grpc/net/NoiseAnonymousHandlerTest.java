package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.southernstorm.noise.protocol.CipherStatePair;
import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

class NoiseAnonymousHandlerTest extends AbstractNoiseHandlerTest {

  @Override
  protected NoiseAnonymousHandler getHandler(final ECKeyPair serverKeyPair) {

    return new NoiseAnonymousHandler(serverKeyPair);
  }

  @Override
  protected CipherStatePair doHandshake() throws Exception {
    return doHandshake(new byte[0]);
  }

  private CipherStatePair doHandshake(final byte[] requestPayload) throws Exception {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    final HandshakeState clientHandshakeState =
        new HandshakeState(HandshakePattern.NK.protocol(), HandshakeState.INITIATOR);

    clientHandshakeState.getRemotePublicKey().setPublicKey(serverKeyPair.getPublicKey().getPublicKeyBytes(), 0);
    clientHandshakeState.start();

    // Send initiator handshake message

    // 32 byte key, request payload, 16 byte AEAD tag
    final int initiateHandshakeMessageLength = 32 + requestPayload.length + 16;
    final byte[] initiateHandshakeMessage = new byte[initiateHandshakeMessageLength];
    assertEquals(
        initiateHandshakeMessageLength,
        clientHandshakeState.writeMessage(initiateHandshakeMessage, 0, requestPayload, 0, requestPayload.length));

    final BinaryWebSocketFrame initiateHandshakeFrame = new BinaryWebSocketFrame(
        Unpooled.wrappedBuffer(initiateHandshakeMessage));

    assertTrue(embeddedChannel.writeOneInbound(initiateHandshakeFrame).await().isSuccess());
    assertEquals(0, initiateHandshakeFrame.refCnt());

    embeddedChannel.runPendingTasks();

    // Read responder handshake message
    assertFalse(embeddedChannel.outboundMessages().isEmpty());
    final BinaryWebSocketFrame responderHandshakeFrame = (BinaryWebSocketFrame)
        embeddedChannel.outboundMessages().poll();
    @SuppressWarnings("DataFlowIssue") final byte[] responderHandshakeBytes =
        new byte[responderHandshakeFrame.content().readableBytes()];
    responderHandshakeFrame.content().readBytes(responderHandshakeBytes);

    // ephemeral key, empty encrypted payload AEAD tag
    final byte[] handshakeResponsePayload = new byte[32 + 16];

    assertEquals(0,
        clientHandshakeState.readMessage(
            responderHandshakeBytes, 0, responderHandshakeBytes.length,
            handshakeResponsePayload, 0));

    final byte[] serverPublicKey = new byte[32];
    clientHandshakeState.getRemotePublicKey().getPublicKey(serverPublicKey, 0);
    assertArrayEquals(serverPublicKey, serverKeyPair.getPublicKey().getPublicKeyBytes());

    return clientHandshakeState.split();
  }

  @Test
  void handleCompleteHandshakeWithRequest() throws ShortBufferException, BadPaddingException {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    assertNotNull(embeddedChannel.pipeline().get(NoiseAnonymousHandler.class));

    final CipherStatePair cipherStatePair = assertDoesNotThrow(() -> doHandshake("ping".getBytes()));
    final byte[] response = readNextPlaintext(cipherStatePair);
    assertArrayEquals(response, "pong".getBytes());

    assertEquals(new NoiseIdentityDeterminedEvent(Optional.empty()), getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleCompleteHandshakeNoRequest() throws ShortBufferException, BadPaddingException {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    assertNotNull(embeddedChannel.pipeline().get(NoiseAnonymousHandler.class));

    final CipherStatePair cipherStatePair = assertDoesNotThrow(() -> doHandshake(new byte[0]));
    assertNull(readNextPlaintext(cipherStatePair));

    assertEquals(new NoiseIdentityDeterminedEvent(Optional.empty()), getNoiseHandshakeCompleteEvent());
  }
}
