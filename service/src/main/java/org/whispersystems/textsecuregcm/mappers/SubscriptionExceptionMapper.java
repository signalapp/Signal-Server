/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.storage.SubscriptionException;

public class SubscriptionExceptionMapper implements ExceptionMapper<SubscriptionException> {

  @Override
  public Response toResponse(final SubscriptionException exception) {
    return switch (exception) {
      case SubscriptionException.NotFound e -> new NotFoundException(e.getMessage(), e.getCause()).getResponse();
      case SubscriptionException.Forbidden e -> new ForbiddenException(e.getMessage(), e.getCause()).getResponse();
      case SubscriptionException.InvalidArguments e ->
          new BadRequestException(e.getMessage(), e.getCause()).getResponse();
      case SubscriptionException.ProcessorConflict e ->
          new ClientErrorException("existing processor does not match", Response.Status.CONFLICT).getResponse();
      default -> new InternalServerErrorException(exception.getMessage(), exception.getCause()).getResponse();
    };
  }
}
