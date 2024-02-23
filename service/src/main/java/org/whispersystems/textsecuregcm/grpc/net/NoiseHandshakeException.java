package org.whispersystems.textsecuregcm.grpc.net;

/**
 * Indicates that some problem occurred while completing a Noise handshake (e.g. an unexpected message size/format or
 * a general encryption error).
 */
class NoiseHandshakeException extends Exception {

  public NoiseHandshakeException(final String message) {
    super(message);
  }
}
