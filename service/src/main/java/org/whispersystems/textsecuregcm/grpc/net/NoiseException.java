package org.whispersystems.textsecuregcm.grpc.net;

import org.whispersystems.textsecuregcm.util.NoStackTraceException;

/**
 * Indicates that some problem occurred while processing an encrypted noise message (e.g. an unexpected message size/
 * format or a general encryption error).
 */
public class NoiseException extends NoStackTraceException {
  public NoiseException(final String message) {
    super(message);
  }
}
