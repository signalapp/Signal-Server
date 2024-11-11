/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceSenderException;

public class RegistrationServiceSenderExceptionMapper implements ExceptionMapper<RegistrationServiceSenderException> {

  public static int REMOTE_SERVICE_REJECTED_REQUEST_STATUS = 440;

  @Override
  public Response toResponse(final RegistrationServiceSenderException exception) {
    return Response.status(REMOTE_SERVICE_REJECTED_REQUEST_STATUS)
        .entity(new SendVerificationCodeFailureResponse(exception.getReason(), exception.isPermanent()))
        .build();
  }

  @VisibleForTesting
  public record SendVerificationCodeFailureResponse(RegistrationServiceSenderException.Reason reason,
                                                    boolean permanentFailure) {

  }
}
