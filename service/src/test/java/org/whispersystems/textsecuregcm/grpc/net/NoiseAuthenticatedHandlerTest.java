package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.southernstorm.noise.protocol.CipherStatePair;
import com.southernstorm.noise.protocol.HandshakeState;
import com.southernstorm.noise.protocol.Noise;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.internal.EmptyArrays;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class NoiseAuthenticatedHandlerTest extends AbstractNoiseHandlerTest {

  private ClientPublicKeysManager clientPublicKeysManager;
  private final ECKeyPair clientKeyPair = Curve.generateKeyPair();

  @Override
  @BeforeEach
  void setUp() {
    clientPublicKeysManager = mock(ClientPublicKeysManager.class);

    super.setUp();
  }

  @Override
  protected NoiseAuthenticatedHandler getHandler(final ECKeyPair serverKeyPair) {
    return new NoiseAuthenticatedHandler(clientPublicKeysManager, serverKeyPair);
  }

  @Override
  protected CipherStatePair doHandshake() throws Throwable {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));
    return doHandshake(identityPayload(accountIdentifier, deviceId));
  }

  @Test
  void handleCompleteHandshakeNoInitialRequest() throws Throwable {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    assertNull(readNextPlaintext(doHandshake(identityPayload(accountIdentifier, deviceId))));

    assertEquals(new NoiseIdentityDeterminedEvent(Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId))),
        getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleCompleteHandshakeWithInitialRequest() throws Throwable {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    final ByteBuffer bb = ByteBuffer.allocate(17 + 4);
    bb.put(identityPayload(accountIdentifier, deviceId));
    bb.put("ping".getBytes());

    final byte[] response = readNextPlaintext(doHandshake(bb.array()));
    assertEquals(response.length, 4);
    assertEquals(new String(response), "pong");

    assertEquals(new NoiseIdentityDeterminedEvent(Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId))),
        getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleCompleteHandshakeMissingIdentityInformation() {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class));

    assertThrows(NoiseHandshakeException.class, () -> doHandshake(EmptyArrays.EMPTY_BYTES));

    verifyNoInteractions(clientPublicKeysManager);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseClientTransportHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakeMalformedIdentityInformation() {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class));

    // no deviceId byte
    byte[] malformedIdentityPayload = UUIDUtil.toBytes(UUID.randomUUID());
    assertThrows(NoiseHandshakeException.class, () -> doHandshake(malformedIdentityPayload));

    verifyNoInteractions(clientPublicKeysManager);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseClientTransportHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakeUnrecognizedDevice() {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    assertThrows(ClientAuthenticationException.class, () -> doHandshake(identityPayload(accountIdentifier, deviceId)));

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseClientTransportHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakePublicKeyMismatch() {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(Curve.generateKeyPair().getPublicKey())));

    assertThrows(ClientAuthenticationException.class, () -> doHandshake(identityPayload(accountIdentifier, deviceId)));

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");
  }

  @Test
  void handleInvalidExtraWrites() throws NoSuchAlgorithmException, ShortBufferException, InterruptedException {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseAuthenticatedHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);

    final HandshakeState clientHandshakeState = clientHandshakeState();

    final CompletableFuture<Optional<ECPublicKey>> findPublicKeyFuture = new CompletableFuture<>();
    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId)).thenReturn(findPublicKeyFuture);

    final BinaryWebSocketFrame initiatorMessageFrame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(
        initiatorHandshakeMessage(clientHandshakeState, identityPayload(accountIdentifier, deviceId))));
    assertTrue(embeddedChannel.writeOneInbound(initiatorMessageFrame).await().isSuccess());

    // While waiting for the public key, send another message
    final ChannelFuture f = embeddedChannel.writeOneInbound(
        new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[0]))).await();
    assertInstanceOf(NoiseHandshakeException.class, f.exceptionNow());

    findPublicKeyFuture.complete(Optional.of(clientKeyPair.getPublicKey()));
    embeddedChannel.runPendingTasks();

    // shouldn't return any response or error, we've already processed an error
    embeddedChannel.checkException();
    assertNull(embeddedChannel.outboundMessages().poll());
  }

  @Test
  public void handleOversizeHandshakeMessage() {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    final byte[] big = TestRandomUtil.nextBytes(Noise.MAX_PACKET_LEN + 1);
    ByteBuffer.wrap(big)
        .put(UUIDUtil.toBytes(UUID.randomUUID()))
        .put((byte) 0x01);
    assertThrows(NoiseHandshakeException.class, () -> doHandshake(big));
  }

  private HandshakeState clientHandshakeState() throws NoSuchAlgorithmException {
    final HandshakeState clientHandshakeState =
        new HandshakeState(HandshakePattern.IK.protocol(), HandshakeState.INITIATOR);

    clientHandshakeState.getLocalKeyPair().setPrivateKey(clientKeyPair.getPrivateKey().serialize(), 0);
    clientHandshakeState.getRemotePublicKey().setPublicKey(serverKeyPair.getPublicKey().getPublicKeyBytes(), 0);
    clientHandshakeState.start();
    return clientHandshakeState;
  }

  private byte[] initiatorHandshakeMessage(final HandshakeState clientHandshakeState, final byte[] payload)
      throws ShortBufferException {
    // Ephemeral key, encrypted static key, AEAD tag, encrypted payload, AEAD tag
    final byte[] initiatorMessageBytes = new byte[32 + 32 + 16 + payload.length + 16];
    int written = clientHandshakeState.writeMessage(initiatorMessageBytes, 0, payload, 0, payload.length);
    assertEquals(written, initiatorMessageBytes.length);
    return initiatorMessageBytes;
  }

  private byte[] readHandshakeResponse(final HandshakeState clientHandshakeState, final byte[] message)
      throws ShortBufferException, BadPaddingException {

    // 32 byte ephemeral server key, 16 byte AEAD tag for encrypted payload
    final int expectedResponsePayloadLength = message.length - 32 - 16;
    final byte[] responsePayload = new byte[expectedResponsePayloadLength];
    final int responsePayloadLength = clientHandshakeState.readMessage(message, 0, message.length, responsePayload, 0);
    assertEquals(expectedResponsePayloadLength, responsePayloadLength);
    return responsePayload;
  }

  private CipherStatePair doHandshake(final byte[] payload) throws Throwable {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    final HandshakeState clientHandshakeState = clientHandshakeState();
    final byte[] initiatorMessage = initiatorHandshakeMessage(clientHandshakeState, payload);

    final BinaryWebSocketFrame initiatorMessageFrame = new BinaryWebSocketFrame(
        Unpooled.wrappedBuffer(initiatorMessage));
    final ChannelFuture await = embeddedChannel.writeOneInbound(initiatorMessageFrame).await();
    assertEquals(0, initiatorMessageFrame.refCnt());
    if (!await.isSuccess()) {
      throw await.cause();
    }

    // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
    // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
    // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
    // and issue a "handshake complete" event.
    embeddedChannel.runPendingTasks();

    // rethrow if running the task caused an error
    embeddedChannel.checkException();

    assertFalse(embeddedChannel.outboundMessages().isEmpty());

    final BinaryWebSocketFrame serverStaticKeyMessageFrame =
        (BinaryWebSocketFrame) embeddedChannel.outboundMessages().poll();
    @SuppressWarnings("DataFlowIssue") final byte[] serverStaticKeyMessageBytes =
        new byte[serverStaticKeyMessageFrame.content().readableBytes()];
    serverStaticKeyMessageFrame.content().readBytes(serverStaticKeyMessageBytes);

    assertEquals(readHandshakeResponse(clientHandshakeState, serverStaticKeyMessageBytes).length, 0);

    final byte[] serverPublicKey = new byte[32];
    clientHandshakeState.getRemotePublicKey().getPublicKey(serverPublicKey, 0);
    assertArrayEquals(serverPublicKey, serverKeyPair.getPublicKey().getPublicKeyBytes());

    return clientHandshakeState.split();
  }


  private static byte[] identityPayload(final UUID accountIdentifier, final byte deviceId) {
    final ByteBuffer clientIdentityPayloadBuffer = ByteBuffer.allocate(17);
    clientIdentityPayloadBuffer.putLong(accountIdentifier.getMostSignificantBits());
    clientIdentityPayloadBuffer.putLong(accountIdentifier.getLeastSignificantBits());
    clientIdentityPayloadBuffer.put(deviceId);
    clientIdentityPayloadBuffer.flip();
    return clientIdentityPayloadBuffer.array();
  }
}
