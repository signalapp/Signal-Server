/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.mappers;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class IOExceptionMapper implements ExceptionMapper<IOException> {

  private final Logger logger = LoggerFactory.getLogger(IOExceptionMapper.class);

  @Override
  public Response toResponse(IOException e) {
    if (!(e.getCause() instanceof java.util.concurrent.TimeoutException)) {
      logger.warn("IOExceptionMapper", e);
    } else {
      // Some TimeoutExceptions are because the connection is idle, but are only distinguishable using the exception
      // message
      final String message = e.getCause().getMessage();
      final boolean idleTimeout =
          message != null &&
              // org.eclipse.jetty.io.IdleTimeout
              (message.startsWith("Idle timeout expired")
                  // org.eclipse.jetty.http2.HTTP2Session
                  || (message.startsWith("Idle timeout") && message.endsWith("elapsed")));
      if (idleTimeout) {
        return Response.status(Response.Status.REQUEST_TIMEOUT).build();
      }
    }

    return Response.status(503).build();
  }
}
