/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.Noise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.UUID;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.grpc.DeviceIdUtil;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

/**
 * Handles the responder side of a noise handshake and then replaces itself with a  {@link NoiseHandler} which will
 * encrypt/decrypt subsequent data frames
 * <p>
 * The handler expects to receive a single inbound message, a {@link NoiseHandshakeInit} that includes the initiator
 * handshake message, connection metadata, and the type of handshake determined by the framing layer. This handler
 * currently supports two types of handshakes.
 * <p>
 * The first are IK handshakes where the initiator's static public key is authenticated by the responder. The initiator
 * handshake message must contain the ACI and deviceId of the initiator. To be authenticated, the static key provided in
 * the handshake message must match the server's stored public key for the device identified by the provided ACI and
 * deviceId.
 * <p>
 * The second are NK handshakes which are anonymous.
 * <p>
 * Optionally, the initiator can also include an initial request in their payload. If provided, this allows the server
 * to begin processing the request without an initial message delay (fast open).
 * <p>
 * Once the handshake has been validated, a {@link NoiseIdentityDeterminedEvent} will be fired. For an IK handshake,
 * this will include the {@link org.whispersystems.textsecuregcm.auth.AuthenticatedDevice} of the initiator. This
 * handler will then replace itself with a {@link NoiseHandler} with a noise state pair ready to encrypt/decrypt data
 * frames.
 */
public class NoiseHandshakeHandler extends ChannelInboundHandlerAdapter {

  private static final byte[] HANDSHAKE_WRONG_PK = NoiseTunnelProtos.HandshakeResponse.newBuilder()
      .setCode(NoiseTunnelProtos.HandshakeResponse.Code.WRONG_PUBLIC_KEY)
      .build().toByteArray();
  private static final byte[] HANDSHAKE_OK = NoiseTunnelProtos.HandshakeResponse.newBuilder()
      .setCode(NoiseTunnelProtos.HandshakeResponse.Code.OK)
      .build().toByteArray();

  // We might get additional messages while we're waiting to process a handshake, so keep track of where we are
  private boolean receivedHandshakeInit = false;

  private final ClientPublicKeysManager clientPublicKeysManager;
  private final ECKeyPair ecKeyPair;

  public NoiseHandshakeHandler(final ClientPublicKeysManager clientPublicKeysManager, final ECKeyPair ecKeyPair) {
    this.clientPublicKeysManager = clientPublicKeysManager;
    this.ecKeyPair = ecKeyPair;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    try {
      if (!(message instanceof NoiseHandshakeInit handshakeInit)) {
        // Anything except HandshakeInit should have been filtered out of the pipeline by now; treat this as an error
        throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
      }
      if (receivedHandshakeInit) {
        throw new NoiseHandshakeException("Should not receive messages until handshake complete");
      }
      receivedHandshakeInit = true;

      if (handshakeInit.content().readableBytes() > Noise.MAX_PACKET_LEN) {
        throw new NoiseHandshakeException("Invalid noise message length " + handshakeInit.content().readableBytes());
      }

      // We've read this frame off the wire, and so it's most likely a direct buffer that's not backed by an array.
      // We'll need to copy it to a heap buffer
      handleInboundHandshake(context,
          handshakeInit.getRemoteAddress(),
          handshakeInit.getHandshakePattern(),
          ByteBufUtil.getBytes(handshakeInit.content()));
    } finally {
      ReferenceCountUtil.release(message);
    }
  }

  private void handleInboundHandshake(
      final ChannelHandlerContext context,
      final InetAddress remoteAddress,
      final HandshakePattern handshakePattern,
      final byte[] frameBytes) throws NoiseHandshakeException {
    final NoiseHandshakeHelper handshakeHelper = new NoiseHandshakeHelper(handshakePattern, ecKeyPair);
    final ByteBuf payload = handshakeHelper.read(frameBytes);

    // Parse the handshake message
    final NoiseTunnelProtos.HandshakeInit handshakeInit;
    try {
      handshakeInit = NoiseTunnelProtos.HandshakeInit.parseFrom(new ByteBufInputStream(payload));
    } catch (IOException e) {
      throw new NoiseHandshakeException("Failed to parse handshake message");
    }

    switch (handshakePattern) {
      case NK -> {
        if (handshakeInit.getDeviceId() != 0 || !handshakeInit.getAci().isEmpty()) {
          throw new NoiseHandshakeException("Anonymous handshake should not include identifiers");
        }
        handleAuthenticated(context, handshakeHelper, remoteAddress, handshakeInit, Optional.empty());
      }
      case IK -> {
        final byte[] publicKeyFromClient = handshakeHelper.remotePublicKey()
            .orElseThrow(() -> new IllegalStateException("No remote public key"));
        final UUID accountIdentifier = aci(handshakeInit);
        final byte deviceId = deviceId(handshakeInit);
        clientPublicKeysManager
            .findPublicKey(accountIdentifier, deviceId)
            .whenCompleteAsync((storedPublicKey, throwable) -> {
              if (throwable != null) {
                context.fireExceptionCaught(ExceptionUtils.unwrap(throwable));
                return;
              }
              final boolean valid = storedPublicKey
                  .map(spk -> MessageDigest.isEqual(publicKeyFromClient, spk.getPublicKeyBytes()))
                  .orElse(false);
              if (!valid) {
                // Write a handshake response indicating that the client used the wrong public key
                final byte[] handshakeMessage = handshakeHelper.write(HANDSHAKE_WRONG_PK);
                context.writeAndFlush(Unpooled.wrappedBuffer(handshakeMessage))
                    .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

                context.fireExceptionCaught(new NoiseHandshakeException("Bad public key"));
                return;
              }
              handleAuthenticated(context,
                  handshakeHelper, remoteAddress, handshakeInit,
                  Optional.of(new AuthenticatedDevice(accountIdentifier, deviceId)));
            }, context.executor());
      }
    };
  }

  private void handleAuthenticated(final ChannelHandlerContext context,
      final NoiseHandshakeHelper handshakeHelper,
      final InetAddress remoteAddress,
      final NoiseTunnelProtos.HandshakeInit handshakeInit,
      final Optional<AuthenticatedDevice> maybeAuthenticatedDevice) {
    context.fireUserEventTriggered(new NoiseIdentityDeterminedEvent(
        maybeAuthenticatedDevice,
        remoteAddress,
        handshakeInit.getUserAgent(),
        handshakeInit.getAcceptLanguage()));

    // Now that we've authenticated, write the handshake response
    final byte[] handshakeMessage = handshakeHelper.write(HANDSHAKE_OK);
    context.writeAndFlush(Unpooled.wrappedBuffer(handshakeMessage))
        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

    // The handshake is complete. We can start intercepting read/write for noise encryption/decryption
    // Note: It may be tempting to swap the before/remove for a replace, but then when we forward the fast open
    // request it will go through the NoiseHandler. We want to skip the NoiseHandler because we've already
    // decrypted the fastOpen request
    context.pipeline()
        .addBefore(context.name(), null, new NoiseHandler(handshakeHelper.getHandshakeState().split()));
    context.pipeline().remove(NoiseHandshakeHandler.class);
    if (!handshakeInit.getFastOpenRequest().isEmpty()) {
      // The handshake had a fast-open request. Forward the plaintext of the request to the server, we'll
      // encrypt the response when the server writes back through us
      context.fireChannelRead(Unpooled.wrappedBuffer(handshakeInit.getFastOpenRequest().asReadOnlyByteBuffer()));
    }
  }

  private static UUID aci(final NoiseTunnelProtos.HandshakeInit handshakePayload) throws NoiseHandshakeException {
    try {
      return UUIDUtil.fromByteString(handshakePayload.getAci());
    } catch (IllegalArgumentException e) {
      throw new NoiseHandshakeException("Could not parse aci");
    }
  }

  private static byte deviceId(final NoiseTunnelProtos.HandshakeInit handshakePayload) throws NoiseHandshakeException {
    if (!DeviceIdUtil.isValid(handshakePayload.getDeviceId())) {
      throw new NoiseHandshakeException("Invalid deviceId");
    }
    return (byte) handshakePayload.getDeviceId();
  }
}
