/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.controllers.ServerRejectedException;

public class ServerRejectedExceptionMapper implements ExceptionMapper<ServerRejectedException> {

  @Override
  public Response toResponse(final ServerRejectedException exception) {
    return Response.status(508).build();
  }
}
