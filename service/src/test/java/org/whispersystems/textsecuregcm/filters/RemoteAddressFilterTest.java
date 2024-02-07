/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import java.util.Optional;
import java.util.stream.Stream;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

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

    final RemoteAddressFilter filter = new RemoteAddressFilter(true);

    final FilterChain filterChain = mock(FilterChain.class);
    filter.doFilter(httpServletRequest, mock(ServletResponse.class), filterChain);

    verify(httpServletRequest).setAttribute(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME, expectedRemoteAddr);
    verify(filterChain).doFilter(any(ServletRequest.class), any(ServletResponse.class));
  }

  @ParameterizedTest
  @CsvSource(value = {
      "192.168.1.1, 127.0.0.1 \t 127.0.0.1",
      "192.168.1.1, 0:0:0:0:0:0:0:1 \t 0:0:0:0:0:0:0:1"
  }, delimiterString = "\t")
  void testGetRemoteAddressFromHeader(final String forwardedFor, final String expectedRemoteAddr) throws Exception {
    final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getHeader(HttpHeaders.X_FORWARDED_FOR)).thenReturn(forwardedFor);

    final RemoteAddressFilter filter = new RemoteAddressFilter(false);

    final FilterChain filterChain = mock(FilterChain.class);
    filter.doFilter(httpServletRequest, mock(ServletResponse.class), filterChain);

    verify(httpServletRequest).setAttribute(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME, expectedRemoteAddr);
    verify(filterChain).doFilter(any(ServletRequest.class), any(ServletResponse.class));
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource("argumentsForGetMostRecentProxy")
  void getMostRecentProxy(final String forwardedFor, final Optional<String> expectedMostRecentProxy) {
    assertEquals(expectedMostRecentProxy, RemoteAddressFilter.getMostRecentProxy(forwardedFor));
  }

  private static Stream<Arguments> argumentsForGetMostRecentProxy() {
    return Stream.of(
        arguments(null, Optional.empty()),
        arguments("", Optional.empty()),
        arguments("    ", Optional.empty()),
        arguments("203.0.113.195,", Optional.empty()),
        arguments("203.0.113.195, ", Optional.empty()),
        arguments("203.0.113.195", Optional.of("203.0.113.195")),
        arguments("203.0.113.195, 70.41.3.18, 150.172.238.178", Optional.of("150.172.238.178"))
    );
  }

}
