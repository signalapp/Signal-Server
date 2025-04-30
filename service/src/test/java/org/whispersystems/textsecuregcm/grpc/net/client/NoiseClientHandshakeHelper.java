/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.client;

import com.southernstorm.noise.protocol.CipherStatePair;
import com.southernstorm.noise.protocol.HandshakeState;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.grpc.net.HandshakePattern;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeException;

public class NoiseClientHandshakeHelper {

  private final HandshakePattern handshakePattern;
  private final HandshakeState handshakeState;

  private NoiseClientHandshakeHelper(HandshakePattern handshakePattern, HandshakeState handshakeState) {
    this.handshakePattern = handshakePattern;
    this.handshakeState = handshakeState;
  }

  public static NoiseClientHandshakeHelper IK(ECPublicKey serverStaticKey, ECKeyPair clientStaticKey) {
    try {
      final HandshakeState state = new HandshakeState(HandshakePattern.IK.protocol(), HandshakeState.INITIATOR);
      state.getLocalKeyPair().setPrivateKey(clientStaticKey.getPrivateKey().serialize(), 0);
      state.getRemotePublicKey().setPublicKey(serverStaticKey.getPublicKeyBytes(), 0);
      state.start();
      return new NoiseClientHandshakeHelper(HandshakePattern.IK, state);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static NoiseClientHandshakeHelper NK(ECPublicKey serverStaticKey) {
    try {
      final HandshakeState state = new HandshakeState(HandshakePattern.NK.protocol(), HandshakeState.INITIATOR);
      state.getRemotePublicKey().setPublicKey(serverStaticKey.getPublicKeyBytes(), 0);
      state.start();
      return new NoiseClientHandshakeHelper(HandshakePattern.NK, state);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public byte[] write(final byte[] requestPayload) throws ShortBufferException {
    final byte[] initiateHandshakeMessage = new byte[initiateHandshakeKeysLength() + requestPayload.length + 16];
    handshakeState.writeMessage(initiateHandshakeMessage, 0, requestPayload, 0, requestPayload.length);
    return initiateHandshakeMessage;
  }

  private int initiateHandshakeKeysLength() {
    return switch (handshakePattern) {
      // 32-byte ephemeral key, 32-byte encrypted static key, 16-byte AEAD tag
      case IK -> 32 + 32 + 16;
      // 32-byte ephemeral key
      case NK -> 32;
    };
  }

  public byte[] read(final byte[] responderHandshakeMessage) throws NoiseHandshakeException {
    // Don't process additional messages if the handshake failed and we're just waiting to close
    if (handshakeState.getAction() != HandshakeState.READ_MESSAGE) {
      throw new NoiseHandshakeException("Received message with handshake state " + handshakeState.getAction());
    }
    final int payloadLength = responderHandshakeMessage.length - 16 - 32;
    final byte[] responsePayload = new byte[payloadLength];
    final int payloadBytesRead;
    try {
      payloadBytesRead = handshakeState
          .readMessage(responderHandshakeMessage, 0, responderHandshakeMessage.length, responsePayload, 0);
      if (payloadBytesRead != responsePayload.length) {
        throw new IllegalStateException(
            "Unexpected payload length, required " + payloadLength + " got " + payloadBytesRead);
      }
      return responsePayload;
    } catch (ShortBufferException e) {
      throw new IllegalStateException("Failed to deserialize payload of known length" + e.getMessage());
    } catch (BadPaddingException e) {
      throw new NoiseHandshakeException(e.getMessage());
    }
  }

  public CipherStatePair split() {
    return this.handshakeState.split();
  }

  public void destroy() {
    this.handshakeState.destroy();
  }
}
