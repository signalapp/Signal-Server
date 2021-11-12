/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;

@PreMatching
public class AcceptNumericOnlineFlagRequestFilter implements ContainerRequestFilter {

  private final String pathPrefix;

  public AcceptNumericOnlineFlagRequestFilter(final String pathPrefix) {
    this.pathPrefix = pathPrefix;
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    if (requestContext.getUriInfo().getPath().startsWith(pathPrefix)) {
      if ("1".equals(requestContext.getUriInfo().getQueryParameters().getFirst("online"))) {
        requestContext.setRequestUri(requestContext.getUriInfo().getRequestUriBuilder()
            .replaceQueryParam("online", "true")
            .build());
      }
    }
  }
}
