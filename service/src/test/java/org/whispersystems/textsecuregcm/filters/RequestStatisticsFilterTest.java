/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.metrics.TrafficSource;

class RequestStatisticsFilterTest {

  @Test
  void testFilter() throws Exception {

    final RequestStatisticsFilter requestStatisticsFilter = new RequestStatisticsFilter(TrafficSource.WEBSOCKET);

    final ContainerRequestContext requestContext = mock(ContainerRequestContext.class);

    when(requestContext.getLength()).thenReturn(-1);
    when(requestContext.getLength()).thenReturn(Integer.MAX_VALUE);
    when(requestContext.getLength()).thenThrow(RuntimeException.class);

    requestStatisticsFilter.filter(requestContext);
    requestStatisticsFilter.filter(requestContext);
    requestStatisticsFilter.filter(requestContext);
  }
}
