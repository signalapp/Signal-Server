/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.logging;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.jersey.errors.LoggingExceptionMapper;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.slf4j.Logger;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

public class LoggingUnhandledExceptionMapper extends LoggingExceptionMapper<Throwable> {

  @Context
  private HttpServletRequest request;

  @Context
  private ExtendedUriInfo uriInfo;

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
      // request and uriInfo shouldn’t be `null`, but it is technically possible
      requestMethod = request.getMethod();
      requestPath = UriInfoUtil.getPathTemplate(uriInfo);
      userAgent = request.getHeader("user-agent");

      // streamline the user-agent if it is recognized
      final UserAgent ua = UserAgentUtil.parseUserAgentString(userAgent);
      userAgent = String.format("%s %s", ua.getPlatform(), ua.getVersion());
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
