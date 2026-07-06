package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.util.NoStackTraceException;

public class InvalidRegistrationSessionException extends NoStackTraceException {

  public InvalidRegistrationSessionException(final String message) {
    super(message);
  }
}
