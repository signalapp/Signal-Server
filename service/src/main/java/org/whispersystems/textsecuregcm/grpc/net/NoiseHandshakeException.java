package org.whispersystems.textsecuregcm.grpc.net;

import org.whispersystems.textsecuregcm.util.NoStackTraceException;

/**
 * Indicates that some problem occurred while completing a Noise handshake (e.g. an unexpected message size/format or
 * a general encryption error).
 */
public class NoiseHandshakeException extends NoStackTraceException {

  public NoiseHandshakeException(final String message) {
    super(message);
  }
}
