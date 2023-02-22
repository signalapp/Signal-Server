/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import com.google.common.annotations.VisibleForTesting;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceSenderException;

public class RegistrationServiceSenderExceptionMapper implements ExceptionMapper<RegistrationServiceSenderException> {

  @Override
  public Response toResponse(final RegistrationServiceSenderException exception) {
    return Response.status(Response.Status.BAD_GATEWAY)
        .entity(new SendVerificationCodeFailureResponse(exception.getReason(), exception.isPermanent()))
        .build();
  }

  @VisibleForTesting
  public record SendVerificationCodeFailureResponse(RegistrationServiceSenderException.Reason reason,
                                                    boolean permanentFailure) {

  }
}
