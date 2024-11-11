/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import io.dropwizard.jersey.errors.ErrorMessage;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class GrpcStatusRuntimeExceptionMapper implements ExceptionMapper<StatusRuntimeException> {

  @Override
  public Response toResponse(final StatusRuntimeException exception) {
    int httpCode = switch (exception.getStatus().getCode()) {
      case OK -> 200;
      case INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE -> 400;
      case UNAUTHENTICATED -> 401;
      case PERMISSION_DENIED -> 403;
      case NOT_FOUND -> 404;
      case ALREADY_EXISTS, ABORTED -> 409;
      case CANCELLED -> 499;
      case UNKNOWN, UNIMPLEMENTED, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, INTERNAL, UNAVAILABLE, DATA_LOSS -> 500;
    };

    return Response.status(httpCode)
        .entity(new ErrorMessage(httpCode, exception.getMessage()))
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }
}
