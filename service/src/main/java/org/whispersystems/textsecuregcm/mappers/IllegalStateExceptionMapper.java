/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import io.dropwizard.jersey.errors.ErrorMessage;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.logging.ImpossibleEvents;

@Provider
public class IllegalStateExceptionMapper implements ExceptionMapper<IllegalStateException> {

  private static final Logger logger = LoggerFactory.getLogger(IllegalStateExceptionMapper.class);

  @Override
  public Response toResponse(final IllegalStateException exception) {
    ImpossibleEvents.logImpossible(logger, exception.getMessage(), exception);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(new ErrorMessage(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            "There was an internal server error processing your request"))
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }
}
