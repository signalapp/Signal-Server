/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.glassfish.jersey.spi.ExceptionMappers;

@Provider
public class CompletionExceptionMapper implements ExceptionMapper<CompletionException> {

  @Context
  private ExceptionMappers exceptionMappers;

  @Override
  public Response toResponse(final CompletionException exception) {
    final Throwable cause = exception.getCause();

    if (cause != null) {

      final ExceptionMapper exceptionMapper = exceptionMappers.findMapping(cause);

      // some exception mappers, like LoggingExceptionMapper, have side effects (e.g., logging)
      // so we always build their response…
      final Response exceptionMapperResponse = exceptionMapper.toResponse(cause);

      final Optional<Response> webApplicationExceptionResponse;
      if (cause instanceof WebApplicationException webApplicationException) {
        webApplicationExceptionResponse = Optional.of(webApplicationException.getResponse());
      } else {
        webApplicationExceptionResponse = Optional.empty();
      }

      // …but if the exception was a WebApplicationException, and provides an entity, we want to keep it
      return webApplicationExceptionResponse
          .filter(Response::hasEntity)
          .orElse(exceptionMapperResponse);
    }

    return Response.serverError().build();
  }
}
