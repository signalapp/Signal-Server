/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.HeaderUtils;

class TimestampResponseFilterTest {

  @Test
  void testJerseyFilter() {
    final ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    final ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    final MultivaluedMap<String, Object> headers = org.glassfish.jersey.message.internal.HeaderUtils.createOutbound();
    when(responseContext.getHeaders()).thenReturn(headers);

    new TimestampResponseFilter().filter(requestContext, responseContext);

    assertTrue(headers.containsKey(org.whispersystems.textsecuregcm.util.HeaderUtils.TIMESTAMP_HEADER));
  }

  @Test
  void testServletFilter() throws Exception {
    final HttpServletRequest request = mock(HttpServletRequest.class);
    final HttpServletResponse response = mock(HttpServletResponse.class);

    new TimestampResponseFilter().doFilter(request, response, mock(FilterChain.class));

    verify(response).setHeader(eq(HeaderUtils.TIMESTAMP_HEADER), matches("\\d{10,}"));
  }
}
