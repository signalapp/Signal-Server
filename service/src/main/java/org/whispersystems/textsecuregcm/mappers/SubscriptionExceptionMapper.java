/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.jersey.errors.ErrorMessage;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import java.util.Map;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;

public class SubscriptionExceptionMapper implements ExceptionMapper<SubscriptionException> {
  @VisibleForTesting
  public static final int PROCESSOR_ERROR_STATUS_CODE = 440;

  @Override
  public Response toResponse(final SubscriptionException exception) {

    // Some exceptions have specific error body formats
    if (exception instanceof SubscriptionException.InvalidAmount e) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(Map.of("error", e.getErrorCode()))
          .type(MediaType.APPLICATION_JSON_TYPE)
          .build();
    }
    if (exception instanceof SubscriptionException.ProcessorException e) {
      return Response.status(PROCESSOR_ERROR_STATUS_CODE)
          .entity(Map.of(
              "processor", e.getProcessor().name(),
              "chargeFailure", e.getChargeFailure()
          ))
          .type(MediaType.APPLICATION_JSON_TYPE)
          .build();
    }
    if (exception instanceof SubscriptionException.ChargeFailurePaymentRequired e) {
      return Response
          .status(Response.Status.PAYMENT_REQUIRED)
          .entity(Map.of("chargeFailure", e.getChargeFailure()))
          .type(MediaType.APPLICATION_JSON_TYPE)
          .build();
    }

    // Otherwise, we'll return a generic error message WebApplicationException, with a detailed error if one is provided
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
