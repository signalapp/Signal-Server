/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import org.whispersystems.textsecuregcm.util.ImpossiblePhoneNumberException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class ImpossiblePhoneNumberExceptionMapper implements ExceptionMapper<ImpossiblePhoneNumberException> {

  @Override
  public Response toResponse(final ImpossiblePhoneNumberException exception) {
    return Response.status(Response.Status.BAD_REQUEST).build();
  }
}
