/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RemoteAddressFilterTest {

  @ParameterizedTest
  @CsvSource({
      "127.0.0.1, 127.0.0.1",
      "0:0:0:0:0:0:0:1, 0:0:0:0:0:0:0:1",
      "[0:0:0:0:0:0:0:1], 0:0:0:0:0:0:0:1"
  })
  void testGetRemoteAddress(final String remoteAddr, final String expectedRemoteAddr) throws Exception {
    final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getRemoteAddr()).thenReturn(remoteAddr);

    final RemoteAddressFilter filter = new RemoteAddressFilter();

    final FilterChain filterChain = mock(FilterChain.class);
    filter.doFilter(httpServletRequest, mock(ServletResponse.class), filterChain);

    verify(httpServletRequest).setAttribute(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME, expectedRemoteAddr);
    verify(filterChain).doFilter(any(ServletRequest.class), any(ServletResponse.class));
  }

}
