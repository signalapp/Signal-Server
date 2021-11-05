/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import java.util.concurrent.CompletionException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.Providers;

@Provider
public class CompletionExceptionMapper implements ExceptionMapper<CompletionException> {

  @Context
  private Providers providers;

  @Override
  public Response toResponse(final CompletionException exception) {
    final Throwable cause = exception.getCause();

    if (cause != null) {
      final Class type = cause.getClass();
      return providers.getExceptionMapper(type).toResponse(cause);
    }

    return Response.serverError().build();
  }
}
