package org.whispersystems.textsecuregcm.filters;

import org.whispersystems.textsecuregcm.util.TimestampHeaderUtil;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import java.util.Collections;

/**
 * Injects a timestamp header into all outbound responses.
 */
public class TimestampResponseFilter implements ContainerResponseFilter {

    @Override
    public void filter(final ContainerRequestContext requestContext, final ContainerResponseContext responseContext) {
        responseContext.getStringHeaders().put(TimestampHeaderUtil.TIMESTAMP_HEADER, Collections.singletonList(String.valueOf(System.currentTimeMillis())));
    }
}
