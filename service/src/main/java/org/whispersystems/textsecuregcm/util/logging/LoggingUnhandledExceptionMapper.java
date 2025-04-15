/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.logging;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.dropwizard.jersey.errors.LoggingExceptionMapper;
import jakarta.inject.Provider;
import jakarta.ws.rs.core.Context;
import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

/**
 * Extends {@link LoggingExceptionMapper} to include the method and path in the log message, if they are available.
 */
public class LoggingUnhandledExceptionMapper extends LoggingExceptionMapper<Throwable> {

  @Context
  private Provider<ContainerRequest> request;

  public LoggingUnhandledExceptionMapper() {
    super();
  }

  @VisibleForTesting
  LoggingUnhandledExceptionMapper(final Logger logger) {
    super(logger);
  }

  @Override
  protected String formatLogMessage(final long id, final Throwable exception) {
    String requestMethod = "unknown method";
    String userAgent = "missing";
    String requestPath = "/{unknown path}";
    try {
      // request shouldn’t be `null`, but it is technically possible
      requestMethod = request.get().getMethod();
      requestPath = UriInfoUtil.getPathTemplate(request.get().getUriInfo());
      userAgent = request.get().getHeaderString(HttpHeaders.USER_AGENT);

      // streamline the user-agent if it is recognized
      final UserAgent ua = UserAgentUtil.parseUserAgentString(userAgent);
      userAgent = String.format("%s %s", ua.platform(), ua.version());
    } catch (final UnrecognizedUserAgentException ignored) {

    } catch (final Exception e) {
      logger.warn("Unexpected exception getting request details", e);
    }

    return String.format("%s at %s %s (%s)",
        super.formatLogMessage(id, exception),
        requestMethod,
        requestPath,
        userAgent) ;
  }

}
