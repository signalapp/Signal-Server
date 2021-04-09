/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import org.whispersystems.textsecuregcm.util.TimestampHeaderUtil;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

/**
 * Injects a timestamp header into all outbound responses.
 */
public class TimestampResponseFilter implements ContainerResponseFilter {

    @Override
    public void filter(final ContainerRequestContext requestContext, final ContainerResponseContext responseContext) {
        responseContext.getHeaders().add(TimestampHeaderUtil.TIMESTAMP_HEADER, System.currentTimeMillis());
    }
}
