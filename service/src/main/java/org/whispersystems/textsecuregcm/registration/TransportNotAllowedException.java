package org.whispersystems.textsecuregcm.registration;

import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;

/**
 * Indicates that a request to send a verification code failed because the destination number does not support the
 * requested transport (e.g. the caller asked to send an SMS to a landline number).
 */
public class TransportNotAllowedException extends RegistrationServiceException {

  public TransportNotAllowedException(RegistrationServiceSession registrationServiceSession) {
    super(registrationServiceSession);
  }
}
