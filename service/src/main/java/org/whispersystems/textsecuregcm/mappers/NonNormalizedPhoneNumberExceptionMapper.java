/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import org.whispersystems.textsecuregcm.util.NonNormalizedPhoneNumberException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

public class NonNormalizedPhoneNumberExceptionMapper implements ExceptionMapper<NonNormalizedPhoneNumberException> {

  @Override
  public Response toResponse(final NonNormalizedPhoneNumberException exception) {
    return Response.status(Status.BAD_REQUEST)
        .entity(new NonNormalizedPhoneNumberResponse(exception.getOriginalNumber(), exception.getNormalizedNumber()))
        .build();
  }
}
