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
import org.whispersystems.textsecuregcm.purchases.ChargeFailure;
import org.whispersystems.textsecuregcm.purchases.PurchaseException;
import org.whispersystems.textsecuregcm.purchases.PurchaseForbiddenException;
import org.whispersystems.textsecuregcm.purchases.PurchaseInvalidAmountException;
import org.whispersystems.textsecuregcm.purchases.PurchaseInvalidArgumentsException;
import org.whispersystems.textsecuregcm.purchases.PurchaseNotFoundException;
import org.whispersystems.textsecuregcm.purchases.PurchasePaymentRequiredException;
import org.whispersystems.textsecuregcm.purchases.PurchaseProcessorConflictException;
import org.whispersystems.textsecuregcm.purchases.PurchaseProcessorException;
import org.whispersystems.textsecuregcm.purchases.PurchaseReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.purchases.SubscriberIdCreationNotPermittedException;

public class PurchaseExceptionMapper implements ExceptionMapper<PurchaseException> {
  @VisibleForTesting
  public static final int PROCESSOR_ERROR_STATUS_CODE = 440;

  public record ChargeFailureResponse(String processor, ChargeFailure chargeFailure) {}

  @Override
  public Response toResponse(final PurchaseException exception) {

    // Some exceptions have specific error body formats
    if (exception instanceof PurchaseInvalidAmountException e) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(Map.of("error", e.getErrorCode()))
          .type(MediaType.APPLICATION_JSON_TYPE)
          .build();
    }
    if (exception instanceof PurchaseProcessorException e) {
      return Response.status(PROCESSOR_ERROR_STATUS_CODE)
          .entity(new ChargeFailureResponse(e.getProcessor().name(), e.getChargeFailure()))
          .type(MediaType.APPLICATION_JSON_TYPE)
          .build();
    }
    if (exception instanceof PurchasePaymentRequiredException e && e.getChargeFailure().isPresent()) {
      return Response
          .status(Response.Status.PAYMENT_REQUIRED)
          .entity(new ChargeFailureResponse(e.getProcessor().name(), e.getChargeFailure().get()))
          .type(MediaType.APPLICATION_JSON_TYPE)
          .build();
    }

    // Otherwise, we'll return a generic error message WebApplicationException, with a detailed error if one is provided
    final Response.Status status = (switch (exception) {
      case PurchaseNotFoundException _ -> Response.Status.NOT_FOUND;
      case PurchaseForbiddenException _ -> Response.Status.FORBIDDEN;
      case PurchaseInvalidArgumentsException _ -> Response.Status.BAD_REQUEST;
      case PurchaseProcessorConflictException _ -> Response.Status.CONFLICT;
      case PurchasePaymentRequiredException _ -> Response.Status.PAYMENT_REQUIRED;
      case PurchaseReceiptAlreadyRedeemedException _ -> Response.Status.CONFLICT;
      case SubscriberIdCreationNotPermittedException _ -> Response.Status.UNAUTHORIZED;
      default -> Response.Status.INTERNAL_SERVER_ERROR;
    });

    // If the PurchaseException came with suitable error message, include that in the response body. Otherwise,
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
