/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.message.internal.HeaderUtils;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.TimestampHeaderUtil;

class TimestampResponseFilterTest {

    @Test
    void testFilter() {
        final ContainerRequestContext        requestContext  = mock(ContainerRequestContext.class);
        final ContainerResponseContext       responseContext = mock(ContainerResponseContext.class);

        final MultivaluedMap<String, Object> headers         = HeaderUtils.createOutbound();

        when(responseContext.getHeaders()).thenReturn(headers);

        new TimestampResponseFilter().filter(requestContext, responseContext);

        assertTrue(headers.containsKey(TimestampHeaderUtil.TIMESTAMP_HEADER));
    }
}
