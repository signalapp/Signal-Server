package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.util.NoStackTraceException;

public class RegistrationLockFailureException extends NoStackTraceException {

  private final RegistrationLockFailure failure;

  public RegistrationLockFailureException(final RegistrationLockFailure failure) {
    super();
    this.failure = failure;
  }

  public RegistrationLockFailure getFailure() {
    return failure;
  }
}
