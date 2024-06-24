/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.HandshakeState;
import com.southernstorm.noise.protocol.Noise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

/**
 * Helper for the responder of a 2-message handshake with a pre-shared responder static key
 */
class NoiseHandshakeHelper {

  private final static int AEAD_TAG_LENGTH = 16;
  private final static int KEY_LENGTH = 32;

  private final HandshakePattern handshakePattern;
  private final ECKeyPair serverStaticKeyPair;
  private final HandshakeState handshakeState;

  NoiseHandshakeHelper(HandshakePattern handshakePattern, ECKeyPair serverStaticKeyPair) {
    this.handshakePattern = handshakePattern;
    this.serverStaticKeyPair = serverStaticKeyPair;
    try {
      this.handshakeState = new HandshakeState(handshakePattern.protocol(), HandshakeState.RESPONDER);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Unsupported Noise algorithm: " + handshakePattern.protocol(), e);
    }
  }

  /**
   * Get the length of the initiator's keys
   *
   * @return length of the handshake message sent by the remote party (the initiator) not including the payload
   */
  private int initiatorHandshakeMessageKeyLength() {
    return switch (handshakePattern) {
      // ephemeral key, static key (encrypted), AEAD tag for static key
      case IK -> KEY_LENGTH + KEY_LENGTH + AEAD_TAG_LENGTH;
      // ephemeral key only
      case NK -> KEY_LENGTH;
    };
  }

  HandshakeState getHandshakeState() {
    return this.handshakeState;
  }

  ByteBuf read(byte[] remoteHandshakeMessage) throws NoiseHandshakeException {
    if (handshakeState.getAction() != HandshakeState.NO_ACTION) {
      throw new NoiseHandshakeException("Cannot send more data before handshake is complete");
    }

    // Length for an empty payload
    final int minMessageLength = initiatorHandshakeMessageKeyLength() + AEAD_TAG_LENGTH;
    if (remoteHandshakeMessage.length < minMessageLength || remoteHandshakeMessage.length > Noise.MAX_PACKET_LEN) {
      throw new NoiseHandshakeException("Unexpected ephemeral key message length");
    }

    final int payloadLength = remoteHandshakeMessage.length - initiatorHandshakeMessageKeyLength() - AEAD_TAG_LENGTH;

    // Cryptographically initializing a handshake is expensive, and so we defer it until we're confident the client is
    // making a good-faith effort to perform a handshake (i.e. now). Noise-java in particular will derive a public key
    // from the supplied private key (and will in fact overwrite any previously-set public key when setting a private
    // key), so we just set the private key here.
    handshakeState.getLocalKeyPair().setPrivateKey(serverStaticKeyPair.getPrivateKey().serialize(), 0);
    handshakeState.start();

    int payloadBytesRead;

    try {
      payloadBytesRead = handshakeState.readMessage(remoteHandshakeMessage, 0, remoteHandshakeMessage.length,
          remoteHandshakeMessage, 0);
    } catch (final ShortBufferException e) {
      // This should never happen since we're checking the length of the frame up front
      throw new NoiseHandshakeException("Unexpected client payload");
    } catch (final BadPaddingException e) {
      // We aren't using padding but may get this error if the AEAD tag does not match the encrypted client static key
      // or payload
      throw new NoiseHandshakeException("Invalid keys or payload");
    }
    if (payloadBytesRead != payloadLength) {
      throw new NoiseHandshakeException(
          "Unexpected payload length, required " + payloadLength + " but got " + payloadBytesRead);
    }
    return Unpooled.wrappedBuffer(remoteHandshakeMessage, 0, payloadBytesRead);
  }

  byte[] write(byte[] payload) {
    if (handshakeState.getAction() != HandshakeState.WRITE_MESSAGE) {
      throw new IllegalStateException("Cannot send data before handshake is complete");
    }

    // Currently only support handshake patterns where the server static key is known
    // Send our ephemeral key and the response to the initiator with the encrypted payload
    final byte[] response = new byte[KEY_LENGTH + payload.length + AEAD_TAG_LENGTH];
    try {
      int written = handshakeState.writeMessage(response, 0, payload, 0, payload.length);
      if (written != response.length) {
        throw new IllegalStateException("Unexpected handshake response length");
      }
      return response;
    } catch (final ShortBufferException e) {
      // This should never happen for messages of known length that we control
      throw new IllegalStateException("Key material buffer was too short for message", e);
    }
  }

  Optional<byte[]> remotePublicKey() {
    return Optional.ofNullable(handshakeState.getRemotePublicKey()).map(dhstate -> {
      final byte[] publicKeyFromClient = new byte[handshakeState.getRemotePublicKey().getPublicKeyLength()];
      handshakeState.getRemotePublicKey().getPublicKey(publicKeyFromClient, 0);
      return publicKeyFromClient;
    });
  }
}
