package org.whispersystems.textsecuregcm.grpc.net;

/**
 * Indicates that some problem occurred while processing an encrypted noise message (e.g. an unexpected message size/
 * format or a general encryption error).
 */
class NoiseException extends Exception {
  public NoiseException(final String message) {
    super(message);
  }
}
