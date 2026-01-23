/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import io.dropwizard.jersey.errors.ErrorMessage;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.backup.BackupBadReceiptException;
import org.whispersystems.textsecuregcm.backup.BackupException;
import org.whispersystems.textsecuregcm.backup.BackupFailedZkAuthenticationException;
import org.whispersystems.textsecuregcm.backup.BackupInvalidArgumentException;
import org.whispersystems.textsecuregcm.backup.BackupMissingIdCommitmentException;
import org.whispersystems.textsecuregcm.backup.BackupNotFoundException;
import org.whispersystems.textsecuregcm.backup.BackupPermissionException;
import org.whispersystems.textsecuregcm.backup.BackupWrongCredentialTypeException;

public class BackupExceptionMapper implements ExceptionMapper<BackupException> {

  @Override
  public Response toResponse(final BackupException exception) {
    final Response.Status status = (switch (exception) {
      case BackupNotFoundException _ -> Response.Status.NOT_FOUND;
      case BackupInvalidArgumentException _, BackupBadReceiptException _ -> Response.Status.BAD_REQUEST;
      case BackupPermissionException _ -> Response.Status.FORBIDDEN;
      case BackupMissingIdCommitmentException _ -> Response.Status.CONFLICT;
      case BackupWrongCredentialTypeException _,
           BackupFailedZkAuthenticationException _ -> Response.Status.UNAUTHORIZED;
      default -> Response.Status.INTERNAL_SERVER_ERROR;
    });

    final WebApplicationException wae =
        new WebApplicationException(exception.getMessage(), exception, Response.status(status).build());

    return Response
        .fromResponse(wae.getResponse())
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(new ErrorMessage(wae.getResponse().getStatus(), wae.getLocalizedMessage())).build();

  }
}
