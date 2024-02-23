package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.southernstorm.noise.protocol.CipherState;
import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.EmptyArrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.storage.Device;

class NoiseXXHandshakeHandlerTest extends AbstractNoiseHandshakeHandlerTest {

  private ClientPublicKeysManager clientPublicKeysManager;

  @Override
  @BeforeEach
  void setUp() {
    clientPublicKeysManager = mock(ClientPublicKeysManager.class);

    super.setUp();
  }

  @Override
  protected NoiseXXHandshakeHandler getHandler(final ECKeyPair serverKeyPair,
      final byte[] serverPublicKeySignature) {

    return new NoiseXXHandshakeHandler(clientPublicKeysManager, serverKeyPair, serverPublicKeySignature);
  }

  @Test
  void handleCompleteHandshake()
      throws ShortBufferException, NoSuchAlgorithmException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    final HandshakeState clientHandshakeState = exchangeClientEphemeralAndServerStaticMessages(clientKeyPair);
    sendClientStaticKey(clientHandshakeState, accountIdentifier, deviceId);

    // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
    // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
    // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
    // and issue a "handshake complete" event.
    embeddedChannel.runPendingTasks();

    assertEquals(new NoiseHandshakeCompleteEvent(Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId))),
        getNoiseHandshakeCompleteEvent());

    assertNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class),
        "Handshake handler should remove self from pipeline after successful handshake");

    assertNotNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
        "Handshake handler should insert a Noise stream handler after successful handshake");
  }

  @Test
  void handleCompleteHandshakeMissingIdentityInformation()
      throws ShortBufferException, NoSuchAlgorithmException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    final HandshakeState clientHandshakeState = exchangeClientEphemeralAndServerStaticMessages(clientKeyPair);

    {
      final byte[] clientStaticKeyMessageBytes = new byte[64];
      final int messageLength =
          clientHandshakeState.writeMessage(clientStaticKeyMessageBytes, 0, EmptyArrays.EMPTY_BYTES, 0, 0);

      assertEquals(clientStaticKeyMessageBytes.length, messageLength);

      final BinaryWebSocketFrame clientStaticKeyMessageFrame =
          new BinaryWebSocketFrame(Unpooled.wrappedBuffer(clientStaticKeyMessageBytes));

      final ChannelFuture writeClientStaticKeyMessageFuture =
          getEmbeddedChannel().writeOneInbound(clientStaticKeyMessageFrame).await();

      assertFalse(writeClientStaticKeyMessageFuture.isSuccess());
      assertInstanceOf(NoiseHandshakeException.class, writeClientStaticKeyMessageFuture.cause());
      assertEquals(0, clientStaticKeyMessageFrame.refCnt());
    }

    // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
    // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
    // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
    // and issue a "handshake complete" event.
    embeddedChannel.runPendingTasks();

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakeMalformedIdentityInformation()
      throws ShortBufferException, NoSuchAlgorithmException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(clientKeyPair.getPublicKey())));

    final HandshakeState clientHandshakeState = exchangeClientEphemeralAndServerStaticMessages(clientKeyPair);

    {
      final byte[] clientStaticKeyMessageBytes = new byte[96];
      final int messageLength =
          clientHandshakeState.writeMessage(clientStaticKeyMessageBytes, 0, new byte[32], 0, 32);

      assertEquals(clientStaticKeyMessageBytes.length, messageLength);

      final BinaryWebSocketFrame clientStaticKeyMessageFrame =
          new BinaryWebSocketFrame(Unpooled.wrappedBuffer(clientStaticKeyMessageBytes));

      final ChannelFuture writeClientStaticKeyMessageFuture =
          getEmbeddedChannel().writeOneInbound(clientStaticKeyMessageFrame).await();

      assertFalse(writeClientStaticKeyMessageFuture.isSuccess());
      assertInstanceOf(NoiseHandshakeException.class, writeClientStaticKeyMessageFuture.cause());
      assertEquals(0, clientStaticKeyMessageFrame.refCnt());
    }

    // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
    // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
    // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
    // and issue a "handshake complete" event.
    embeddedChannel.runPendingTasks();

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakeUnrecognizedDevice()
      throws ShortBufferException, NoSuchAlgorithmException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final HandshakeState clientHandshakeState = exchangeClientEphemeralAndServerStaticMessages(clientKeyPair);
    sendClientStaticKey(clientHandshakeState, accountIdentifier, deviceId);

    // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
    // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
    // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
    // and issue a "handshake complete" event.
    embeddedChannel.runPendingTasks();

    assertThrows(ClientAuthenticationException.class, embeddedChannel::checkException);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakePublicKeyMismatch()
      throws ShortBufferException, NoSuchAlgorithmException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(Curve.generateKeyPair().getPublicKey())));

    final HandshakeState clientHandshakeState = exchangeClientEphemeralAndServerStaticMessages(clientKeyPair);
    sendClientStaticKey(clientHandshakeState, accountIdentifier, deviceId);

    // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
    // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
    // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
    // and issue a "handshake complete" event.
    embeddedChannel.runPendingTasks();

    assertThrows(ClientAuthenticationException.class, embeddedChannel::checkException);

    assertNull(getNoiseHandshakeCompleteEvent());

    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class),
        "Handshake handler should not remove self from pipeline after failed handshake");

    assertNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
        "Noise stream handler should not be added to pipeline after failed handshake");
  }

  @Test
  void handleCompleteHandshakeBufferedReads()
      throws ShortBufferException, NoSuchAlgorithmException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    final CompletableFuture<Optional<ECPublicKey>> findPublicKeyFuture = new CompletableFuture<>();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId)).thenReturn(findPublicKeyFuture);

    final HandshakeState clientHandshakeState = exchangeClientEphemeralAndServerStaticMessages(clientKeyPair);
    sendClientStaticKey(clientHandshakeState, accountIdentifier, deviceId);

    final ByteBuf[] additionalMessages = new ByteBuf[4];
    final CipherState senderState = clientHandshakeState.split().getSender();

    try {
      for (int i = 0; i < additionalMessages.length; i++) {
        final byte[] contentBytes = new byte[32];
        ThreadLocalRandom.current().nextBytes(contentBytes);

        // Copy the "plaintext" portion of the content bytes for future assertions
        additionalMessages[i] = Unpooled.buffer(16).writeBytes(contentBytes, 0, 16);

        // Overwrite the first 16 bytes of a random "plaintext" with a ciphertext and the second 16 bytes with the AEAD
        // tag
        senderState.encryptWithAd(null, contentBytes, 0, contentBytes, 0, 16);

        assertTrue(
            embeddedChannel.writeOneInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(contentBytes))).await()
                .isSuccess());
      }

      findPublicKeyFuture.complete(Optional.of(clientKeyPair.getPublicKey()));

      // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
      // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
      // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
      // and issue a "handshake complete" event.
      embeddedChannel.runPendingTasks();

      assertEquals(new NoiseHandshakeCompleteEvent(Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId))),
          getNoiseHandshakeCompleteEvent());

      assertNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class),
          "Handshake handler should remove self from pipeline after successful handshake");

      assertNotNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
          "Handshake handler should insert a Noise stream handler after successful handshake");

      for (final ByteBuf additionalMessage : additionalMessages) {
        assertEquals(additionalMessage, embeddedChannel.inboundMessages().poll(),
            "Buffered message should pass through pipeline after successful handshake");
      }
    } finally {
      for (final ByteBuf additionalMessage : additionalMessages) {
        additionalMessage.release();
      }
    }
  }

  @Test
  void handleCompleteHandshakeFailureBufferedReads()
      throws ShortBufferException, NoSuchAlgorithmException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();
    assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class));

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) ThreadLocalRandom.current().nextInt(Device.MAXIMUM_DEVICE_ID);
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    final CompletableFuture<Optional<ECPublicKey>> findPublicKeyFuture = new CompletableFuture<>();

    when(clientPublicKeysManager.findPublicKey(accountIdentifier, deviceId)).thenReturn(findPublicKeyFuture);

    final HandshakeState clientHandshakeState = exchangeClientEphemeralAndServerStaticMessages(clientKeyPair);
    sendClientStaticKey(clientHandshakeState, accountIdentifier, deviceId);

    final ByteBuf[] additionalMessages = new ByteBuf[4];
    final CipherState senderState = clientHandshakeState.split().getSender();

    try {
      for (int i = 0; i < additionalMessages.length; i++) {
        final byte[] contentBytes = new byte[32];
        ThreadLocalRandom.current().nextBytes(contentBytes);

        // Copy the "plaintext" portion of the content bytes for future assertions
        additionalMessages[i] = Unpooled.buffer(16).writeBytes(contentBytes, 0, 16);

        // Overwrite the first 16 bytes of a random "plaintext" with a ciphertext and the second 16 bytes with the AEAD
        // tag
        senderState.encryptWithAd(null, contentBytes, 0, contentBytes, 0, 16);

        assertTrue(embeddedChannel.writeOneInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(contentBytes))).await().isSuccess());
      }

      findPublicKeyFuture.complete(Optional.empty());

      // The handshake handler makes an asynchronous call to get the stored public key for the client, then handles the
      // result on its event loop. Because this is an embedded channel, this all happens on the main thread (i.e. the same
      // thread as this test), and so we need to nudge things forward to actually process the "found credentials" callback
      // and issue a "handshake complete" event.
      embeddedChannel.runPendingTasks();

      assertNull(getNoiseHandshakeCompleteEvent());

      assertNotNull(embeddedChannel.pipeline().get(NoiseXXHandshakeHandler.class),
          "Handshake handler should not remove self from pipeline after failed handshake");

      assertNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
          "Noise stream handler should not be added to pipeline after failed handshake");

      assertTrue(embeddedChannel.inboundMessages().isEmpty(),
          "Buffered messages should not pass through pipeline after failed handshake");
    } finally {
      for (final ByteBuf additionalMessage : additionalMessages) {
        additionalMessage.release();
      }
    }
  }

  private HandshakeState exchangeClientEphemeralAndServerStaticMessages(final ECKeyPair clientKeyPair)
      throws NoSuchAlgorithmException, ShortBufferException, BadPaddingException, InterruptedException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    final HandshakeState clientHandshakeState =
        new HandshakeState(NoiseXXHandshakeHandler.NOISE_PROTOCOL_NAME, HandshakeState.INITIATOR);

    clientHandshakeState.getLocalKeyPair().setPrivateKey(clientKeyPair.getPrivateKey().serialize(), 0);
    clientHandshakeState.start();

    {
      final byte[] ephemeralKeyMessageBytes = new byte[32];
      clientHandshakeState.writeMessage(ephemeralKeyMessageBytes, 0, null, 0, 0);

      final BinaryWebSocketFrame ephemeralKeyMessageFrame =
          new BinaryWebSocketFrame(Unpooled.wrappedBuffer(ephemeralKeyMessageBytes));

      assertTrue(embeddedChannel.writeOneInbound(ephemeralKeyMessageFrame).await().isSuccess());
      assertEquals(0, ephemeralKeyMessageFrame.refCnt());
    }

    {
      assertEquals(1, embeddedChannel.outboundMessages().size());

      final BinaryWebSocketFrame serverStaticKeyMessageFrame =
          (BinaryWebSocketFrame) embeddedChannel.outboundMessages().poll();

      @SuppressWarnings("DataFlowIssue") final byte[] serverStaticKeyMessageBytes =
          new byte[serverStaticKeyMessageFrame.content().readableBytes()];

      serverStaticKeyMessageFrame.content().readBytes(serverStaticKeyMessageBytes);

      final byte[] serverPublicKeySignature = new byte[64];

      final int payloadLength =
          clientHandshakeState.readMessage(serverStaticKeyMessageBytes, 0, serverStaticKeyMessageBytes.length, serverPublicKeySignature, 0);

      assertEquals(serverPublicKeySignature.length, payloadLength);

      final byte[] serverPublicKey = new byte[32];
      clientHandshakeState.getRemotePublicKey().getPublicKey(serverPublicKey, 0);

      assertTrue(getRootPublicKey().verifySignature(serverPublicKey, serverPublicKeySignature));
    }

    return clientHandshakeState;
  }

  private void sendClientStaticKey(final HandshakeState handshakeState, final UUID accountIdentifier, final byte deviceId)
      throws ShortBufferException, InterruptedException {

    final ByteBuffer clientIdentityPayloadBuffer = ByteBuffer.allocate(17);
    clientIdentityPayloadBuffer.putLong(accountIdentifier.getMostSignificantBits());
    clientIdentityPayloadBuffer.putLong(accountIdentifier.getLeastSignificantBits());
    clientIdentityPayloadBuffer.put(deviceId);
    clientIdentityPayloadBuffer.flip();

    final byte[] clientStaticKeyMessageBytes = new byte[81];
    final int messageLength =
        handshakeState.writeMessage(clientStaticKeyMessageBytes, 0, clientIdentityPayloadBuffer.array(), 0, clientIdentityPayloadBuffer.remaining());

    assertEquals(clientStaticKeyMessageBytes.length, messageLength);

    final BinaryWebSocketFrame clientStaticKeyMessageFrame =
        new BinaryWebSocketFrame(Unpooled.wrappedBuffer(clientStaticKeyMessageBytes));

    assertTrue(getEmbeddedChannel().writeOneInbound(clientStaticKeyMessageFrame).await().isSuccess());
    assertEquals(0, clientStaticKeyMessageFrame.refCnt());
  }
}
