package org.whispersystems.textsecuregcm.filters;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import java.util.Collections;

/**
 * Injects a timestamp header into all outbound responses.
 */
public class TimestampResponseFilter implements ContainerResponseFilter {

    private static final String TIMESTAMP_HEADER = "X-Signal-Timestamp";

    @Override
    public void filter(final ContainerRequestContext requestContext, final ContainerResponseContext responseContext) {
        responseContext.getStringHeaders().put(TIMESTAMP_HEADER, Collections.singletonList(String.valueOf(System.currentTimeMillis())));
    }
}
