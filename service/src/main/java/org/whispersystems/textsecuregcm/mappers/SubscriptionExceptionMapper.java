/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import io.dropwizard.jersey.errors.ErrorMessage;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;

public class SubscriptionExceptionMapper implements ExceptionMapper<SubscriptionException> {

  @Override
  public Response toResponse(final SubscriptionException exception) {
    final Response.Status status = (switch (exception) {
      case SubscriptionException.NotFound e -> Response.Status.NOT_FOUND;
      case SubscriptionException.Forbidden e -> Response.Status.FORBIDDEN;
      case SubscriptionException.InvalidArguments e -> Response.Status.BAD_REQUEST;
      case SubscriptionException.ProcessorConflict e -> Response.Status.CONFLICT;
      case SubscriptionException.PaymentRequired e -> Response.Status.PAYMENT_REQUIRED;
      default -> Response.Status.INTERNAL_SERVER_ERROR;
    });

    // If the SubscriptionException came with suitable error message, include that in the response body. Otherwise,
    // don't provide any message to the WebApplicationException constructor so the response includes the default
    // HTTP error message for the status.
    final WebApplicationException wae = exception.errorDetail()
        .map(errorMessage -> new WebApplicationException(errorMessage, exception, Response.status(status).build()))
        .orElseGet(() -> new WebApplicationException(exception, Response.status(status).build()));

    return Response
        .fromResponse(wae.getResponse())
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(new ErrorMessage(wae.getResponse().getStatus(), wae.getLocalizedMessage())).build();
  }
}
