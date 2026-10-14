/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;

public class CallNotPermittedExceptionMapper implements ExceptionMapper<CallNotPermittedException> {

  @Override
  public Response toResponse(final CallNotPermittedException exception) {
    return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
  }
}
