package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.southernstorm.noise.protocol.CipherStatePair;
import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.junit.jupiter.api.Test;

class NoiseAnonymousHandlerTest extends AbstractNoiseHandlerTest {

  @Override
  protected CipherStatePair doHandshake() throws Exception {
    return doHandshake(baseHandshakeInit().build().toByteArray());
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
    final NoiseHandshakeInit message = new NoiseHandshakeInit(
        REMOTE_ADDRESS,
        HandshakePattern.NK,
        Unpooled.wrappedBuffer(initiateHandshakeMessage));
    assertTrue(embeddedChannel.writeOneInbound(message).await().isSuccess());
    assertEquals(0, message.refCnt());

    embeddedChannel.runPendingTasks();

    // Read responder handshake message
    assertFalse(embeddedChannel.outboundMessages().isEmpty());
    final ByteBuf responderHandshakeFrame = (ByteBuf) embeddedChannel.outboundMessages().poll();
    assertNotNull(responderHandshakeFrame);
    final byte[] responderHandshakeBytes = ByteBufUtil.getBytes(responderHandshakeFrame);

    final NoiseTunnelProtos.HandshakeResponse expectedHandshakeResponse = NoiseTunnelProtos.HandshakeResponse.newBuilder()
        .setCode(NoiseTunnelProtos.HandshakeResponse.Code.OK)
        .build();

    // ephemeral key, payload, AEAD tag
    assertEquals(32 + expectedHandshakeResponse.getSerializedSize() + 16, responderHandshakeBytes.length);

    final byte[] handshakeResponsePlaintext = new byte[expectedHandshakeResponse.getSerializedSize()];
    assertEquals(expectedHandshakeResponse.getSerializedSize(),
        clientHandshakeState.readMessage(
            responderHandshakeBytes, 0, responderHandshakeBytes.length,
            handshakeResponsePlaintext, 0));

    assertEquals(expectedHandshakeResponse, NoiseTunnelProtos.HandshakeResponse.parseFrom(handshakeResponsePlaintext));

    final byte[] serverPublicKey = new byte[32];
    clientHandshakeState.getRemotePublicKey().getPublicKey(serverPublicKey, 0);
    assertArrayEquals(serverPublicKey, serverKeyPair.getPublicKey().getPublicKeyBytes());

    return clientHandshakeState.split();
  }

  @Test
  void handleCompleteHandshakeWithRequest() throws Exception {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    final byte[] handshakePlaintext = baseHandshakeInit()
        .setFastOpenRequest(ByteString.copyFromUtf8("ping")).build()
        .toByteArray();

    final CipherStatePair cipherStatePair = doHandshake(handshakePlaintext);
    final byte[] response = readNextPlaintext(cipherStatePair);
    assertArrayEquals(response, "pong".getBytes());

    assertEquals(
        new NoiseIdentityDeterminedEvent(Optional.empty(), REMOTE_ADDRESS, USER_AGENT, ACCEPT_LANGUAGE),
        getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleCompleteHandshakeNoRequest() throws ShortBufferException, BadPaddingException {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    final CipherStatePair cipherStatePair = assertDoesNotThrow(() -> doHandshake());
    assertNull(readNextPlaintext(cipherStatePair));

    assertEquals(
        new NoiseIdentityDeterminedEvent(Optional.empty(), REMOTE_ADDRESS, USER_AGENT, ACCEPT_LANGUAGE),
        getNoiseHandshakeCompleteEvent());
  }
}
