package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.southernstorm.noise.protocol.CipherStatePair;
import com.southernstorm.noise.protocol.HandshakeState;
import com.southernstorm.noise.protocol.Noise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.internal.EmptyArrays;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.grpc.net.client.NoiseClientTransportHandler;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class NoiseAuthenticatedHandlerTest extends AbstractNoiseHandlerTest {

  private final ECKeyPair clientKeyPair = ECKeyPair.generate();

  @Override
  protected CipherStatePair doHandshake() throws Throwable {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = randomDeviceId();
    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));
    return doHandshake(identityPayload(accountIdentifier, deviceId));
  }

  @Test
  void handleCompleteHandshakeNoInitialRequest() throws Throwable {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = randomDeviceId();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    assertNull(readNextPlaintext(doHandshake(identityPayload(accountIdentifier, deviceId))));

    assertEquals(
        new NoiseIdentityDeterminedEvent(
            Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId)),
            REMOTE_ADDRESS, USER_AGENT, ACCEPT_LANGUAGE),
        getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleCompleteHandshakeWithInitialRequest() throws Throwable {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = randomDeviceId();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    final byte[] handshakeInit = identifiedHandshakeInit(accountIdentifier, deviceId)
        .setFastOpenRequest(ByteString.copyFromUtf8("ping"))
        .build()
        .toByteArray();

    final byte[] response = readNextPlaintext(doHandshake(handshakeInit));
    assertEquals(4, response.length);
    assertEquals("pong", new String(response));

    assertEquals(
        new NoiseIdentityDeterminedEvent(
            Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId)),
            REMOTE_ADDRESS, USER_AGENT, ACCEPT_LANGUAGE),
        getNoiseHandshakeCompleteEvent());
  }

  @Test
  void handleCompleteHandshakeMissingIdentityInformation() {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    assertThrows(NoiseHandshakeException.class, () -> doHandshake(EmptyArrays.EMPTY_BYTES));

    verifyNoInteractions(clientPublicKeysManager);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseClientTransportHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakeMalformedIdentityInformation() {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    // no deviceId byte
    byte[] malformedIdentityPayload = UUIDUtil.toBytes(UUID.randomUUID());
    assertThrows(NoiseHandshakeException.class, () -> doHandshake(malformedIdentityPayload));

    verifyNoInteractions(clientPublicKeysManager);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseClientTransportHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakeUnrecognizedDevice() throws Throwable {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = randomDeviceId();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    doHandshake(
        identityPayload(accountIdentifier, deviceId),
        NoiseTunnelProtos.HandshakeResponse.Code.WRONG_PUBLIC_KEY);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseClientTransportHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakePublicKeyMismatch() throws Throwable {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = randomDeviceId();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ECKeyPair.generate().getPublicKey())));

    doHandshake(
        identityPayload(accountIdentifier, deviceId),
        NoiseTunnelProtos.HandshakeResponse.Code.WRONG_PUBLIC_KEY);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");
  }

  @Test
  void handleInvalidExtraWrites()
      throws NoSuchAlgorithmException, ShortBufferException, InterruptedException {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = randomDeviceId();

    final HandshakeState clientHandshakeState = clientHandshakeState();

    final CompletableFuture<Optional<ECPublicKey>> findPublicKeyFuture = new CompletableFuture<>();
    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId)).thenReturn(findPublicKeyFuture);

    final NoiseHandshakeInit handshakeInit = new NoiseHandshakeInit(
        REMOTE_ADDRESS,
        HandshakePattern.IK,
        Unpooled.wrappedBuffer(
            initiatorHandshakeMessage(clientHandshakeState, identityPayload(accountIdentifier, deviceId))));
    assertTrue(embeddedChannel.writeOneInbound(handshakeInit).await().isSuccess());

    // While waiting for the public key, send another message
    final ChannelFuture f = embeddedChannel.writeOneInbound(Unpooled.wrappedBuffer(new byte[0])).await();
    assertInstanceOf(IllegalArgumentException.class, f.exceptionNow());

    findPublicKeyFuture.complete(Optional.of(clientKeyPair.getPublicKey()));
    embeddedChannel.runPendingTasks();
  }

  @Test
  public void handleOversizeHandshakeMessage() {
    final byte[] big = TestRandomUtil.nextBytes(Noise.MAX_PACKET_LEN + 1);
    ByteBuffer.wrap(big)
        .put(UUIDUtil.toBytes(UUID.randomUUID()))
        .put((byte) 0x01);
    assertThrows(NoiseHandshakeException.class, () -> doHandshake(big));
  }

  @Test
  public void handleKeyLookupError() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = randomDeviceId();
    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.failedFuture(new IOException()));
    assertThrows(IOException.class, () -> doHandshake(identityPayload(accountIdentifier, deviceId)));
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
    return doHandshake(payload, NoiseTunnelProtos.HandshakeResponse.Code.OK);
  }

  private CipherStatePair doHandshake(final byte[] payload, final NoiseTunnelProtos.HandshakeResponse.Code expectedStatus) throws Throwable {
    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    final HandshakeState clientHandshakeState = clientHandshakeState();
    final byte[] initiatorMessage = initiatorHandshakeMessage(clientHandshakeState, payload);

    final NoiseHandshakeInit initMessage = new NoiseHandshakeInit(
        REMOTE_ADDRESS,
        HandshakePattern.IK,
        Unpooled.wrappedBuffer(initiatorMessage));
    final ChannelFuture await = embeddedChannel.writeOneInbound(initMessage).await();
    assertEquals(0, initMessage.refCnt());
    if (!await.isSuccess() && expectedStatus == NoiseTunnelProtos.HandshakeResponse.Code.OK) {
      throw await.cause();
    }

    // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
    // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
    // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
    // and issue a "handshake complete" event.
    embeddedChannel.runPendingTasks();

    // rethrow if running the task caused an error, and the caller isn't expecting an error
    if (expectedStatus == NoiseTunnelProtos.HandshakeResponse.Code.OK) {
      embeddedChannel.checkException();
    }

    assertFalse(embeddedChannel.outboundMessages().isEmpty());

    final ByteBuf handshakeResponseFrame = (ByteBuf) embeddedChannel.outboundMessages().poll();
    assertNotNull(handshakeResponseFrame);
    final byte[] handshakeResponseCiphertextBytes = ByteBufUtil.getBytes(handshakeResponseFrame);

    final NoiseTunnelProtos.HandshakeResponse expectedHandshakeResponsePlaintext = NoiseTunnelProtos.HandshakeResponse.newBuilder()
        .setCode(expectedStatus)
        .build();

    final byte[] actualHandshakeResponsePlaintext =
        readHandshakeResponse(clientHandshakeState, handshakeResponseCiphertextBytes);

    assertEquals(
        expectedHandshakeResponsePlaintext,
        NoiseTunnelProtos.HandshakeResponse.parseFrom(actualHandshakeResponsePlaintext));

    final byte[] serverPublicKey = new byte[32];
    clientHandshakeState.getRemotePublicKey().getPublicKey(serverPublicKey, 0);
    assertArrayEquals(serverPublicKey, serverKeyPair.getPublicKey().getPublicKeyBytes());

    return clientHandshakeState.split();
  }

  private NoiseTunnelProtos.HandshakeInit.Builder identifiedHandshakeInit(final UUID accountIdentifier, final byte deviceId) {
    return baseHandshakeInit()
        .setAci(UUIDUtil.toByteString(accountIdentifier))
        .setDeviceId(deviceId);
  }

  private byte[] identityPayload(final UUID accountIdentifier, final byte deviceId) {
    return identifiedHandshakeInit(accountIdentifier, deviceId)
        .build()
        .toByteArray();
  }

  private static byte randomDeviceId() {
    return (byte) ThreadLocalRandom.current().nextInt(1, Device.MAXIMUM_DEVICE_ID + 1);
  }
}
