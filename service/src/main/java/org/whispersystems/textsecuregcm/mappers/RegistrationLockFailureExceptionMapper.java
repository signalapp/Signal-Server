package org.whispersystems.textsecuregcm.mappers;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.auth.RegistrationLockFailureException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;

public class RegistrationLockFailureExceptionMapper implements ExceptionMapper<RegistrationLockFailureException> {

  @Override
  public Response toResponse(final RegistrationLockFailureException exception) {
    return Response.status(RegistrationLockVerificationManager.FAILURE_HTTP_STATUS)
        .entity(exception.getFailure())
        .build();
  }
}
